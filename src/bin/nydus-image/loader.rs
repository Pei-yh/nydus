use crate::tree::Tree;
use anyhow::{Context, Result};
use nydus_utils::digest::{Algorithm as DigestAlgorithm, RafsDigest};
use rafs::metadata::{RafsMode, RafsSuper};
use rafs::RafsIoReader;
use rusqlite::{params, Connection};
use std::collections::HashSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use storage::compress::{self, Algorithm as CompressAlgorithm};
use storage::device::RafsChunkFlags;

pub struct Loader {
    db_path: PathBuf,
    f_bootstrap: RafsIoReader,
}

impl Loader {
    pub fn new(bootstrap_path: &Path, db_path: &Path) -> Result<Self> {
        let f_bootstrap = Box::new(
            OpenOptions::new()
                .read(true)
                .write(false)
                .open(bootstrap_path)
                .context(format!(
                    "failed to open bootstrap file {:?} for validator",
                    bootstrap_path
                ))?,
        );

        Ok(Self {
            db_path: db_path.to_path_buf(),
            f_bootstrap,
        })
    }

    pub fn load(&mut self, verbosity: bool, blobs_path: &Path) -> Result<Vec<String>> {
        let err = "failed to load bootstrap for validator";
        let mut rs = RafsSuper {
            mode: RafsMode::Direct,
            validate_digest: true,
            ..Default::default()
        };
        rs.load(&mut self.f_bootstrap).context(err)?;

        let tree = Tree::from_bootstrap(&rs, None).context(err)?;

        let mut chunks = vec![];
        tree.iterate(&mut |node| {
            if verbosity && !node.is_dir() && !node.is_symlink() && !node.is_special() {
                let flag = if node.inode.i_size >= 1024 * 1024 {
                    2
                } else if node.inode.i_size >= 256 * 1024 {
                    1
                } else {
                    0
                };
                for chunk in &node.chunks {
                    let chunk = Chunk {
                        block_id: chunk.block_id,
                        blob_index: chunk.blob_index,
                        compress_size: chunk.compress_size,
                        decompress_size: chunk.decompress_size,
                        compress_offset: chunk.compress_offset,
                        decompress_offset: chunk.decompress_offset,
                        is_compress: chunk.flags.contains(RafsChunkFlags::COMPRESSED),
                        flag,
                    };
                    chunks.push(chunk);
                }
            }
            true
        })?;

        let blob_ids = rs
            .superblock
            .get_blob_table()
            .entries
            .iter()
            .map(|entry| entry.blob_id.to_string())
            .collect::<Vec<String>>();

        let mut db = DedupDB::new(
            &self.db_path,
            rs.meta.get_compressor(),
            rs.meta.get_digester(),
        )?;

        db.handle_chunks(&chunks, &blob_ids, &blobs_path)?;

        db.insert_blobs(&blob_ids)?;
        Ok(blob_ids)
    }
}

struct DedupDB {
    compress: CompressAlgorithm,
    digest: DigestAlgorithm,
    conn: Connection,
}

struct Chunk {
    pub block_id: RafsDigest,
    pub blob_index: u32,
    pub compress_size: u32,
    pub decompress_size: u32,
    pub compress_offset: u64,
    pub decompress_offset: u64,
    pub is_compress: bool,
    pub flag: u32,
}

impl DedupDB {
    pub fn new(
        db_path: &Path,
        compress: CompressAlgorithm,
        digest: DigestAlgorithm,
    ) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        conn.execute("PRAGMA temp_store = MEMORY", params![])?;

        // crate table for chunk level deduplication
        for size in &[4, 16, 64, 256, 1024] {
            let chunk_table = format!("chunk_{}kb", size);
            let chunk_sql = format!(
                "CREATE TABLE IF NOT EXISTS {} (
                    hash TEXT PRIMARY KEY,
                    size INT,
                    count INT,
                    flag INT
                )",
                chunk_table
            );
            conn.execute(&chunk_sql, &[] as &[&dyn rusqlite::ToSql])?;
        }

        // crate blob table
        let blob_sql = "
            CREATE TABLE IF NOT EXISTS blob (
                hash TEXT PRIMARY KEY
            )
        ";
        conn.execute(blob_sql, &[] as &[&dyn rusqlite::ToSql])?;

        Ok(DedupDB {
            compress,
            digest,
            conn,
        })
    }

    pub fn handle_chunks(
        &mut self,
        chunks: &[Chunk],
        blob_ids: &[String],
        blobs: &Path,
    ) -> Result<()> {
        let mut chunks_4k = Vec::new();
        let mut chunks_16k = Vec::new();
        let mut chunks_64k = Vec::new();
        let mut chunks_256k = Vec::new();
        let mut chunks_1024k = Vec::new();

        let mut blob_files = Vec::with_capacity(blob_ids.len());
        for blob_id in blob_ids {
            let blob_id_with_prefix = format!("sha256:{}", blob_id);
            let blob_path = blobs.join(blob_id_with_prefix);
            let file = File::open(&blob_path)
                .with_context(|| format!("Failed to open blob file: {}", blob_path.display()))?;
            blob_files.push(file);
        }

        let mut seen_chunks = HashSet::new();
        for chunk in chunks {
            if !seen_chunks.insert(chunk.block_id.to_string()) {
                continue;
            }
            let blob_index = chunk.blob_index as usize;
            let blob_id = &blob_ids[blob_index];

            if blob_index >= blob_files.len() {
                return Err(anyhow::anyhow!(
                    "Invalid blob_index {} for chunk {}",
                    blob_index,
                    chunk.block_id
                ));
            }

            let file = &mut blob_files[blob_index];

            file.seek(SeekFrom::Start(chunk.compress_offset))
                .with_context(|| {
                    format!(
                        "Failed to seek to offset {} in blob {}",
                        chunk.compress_offset, blob_index
                    )
                })?;

            let mut raw_data = vec![0u8; chunk.compress_size as usize];
            file.read_exact(&mut raw_data).with_context(|| {
                format!(
                    "Failed to read {} bytes at offset {} in blob {}",
                    chunk.compress_size, chunk.compress_offset, blob_index
                )
            })?;

            let chunk_data = if chunk.is_compress {
                let mut buf = vec![0u8; chunk.decompress_size as usize];
                compress::decompress(&raw_data, None, &mut buf, self.compress).map_err(|e| {
                    error!("failed to decompress chunk: {}", e);
                    e
                })?;
                buf
            } else {
                raw_data
            };

            chunks_4k.extend(self.split_chunk(&chunk_data, 4, chunk.flag, blob_id)?);
            chunks_16k.extend(self.split_chunk(&chunk_data, 16, chunk.flag, blob_id)?);
            chunks_64k.extend(self.split_chunk(&chunk_data, 64, chunk.flag, blob_id)?);
            chunks_256k.extend(self.split_chunk(&chunk_data, 256, chunk.flag, blob_id)?);
            chunks_1024k.extend(self.split_chunk(&chunk_data, 1024, chunk.flag, blob_id)?);
        }

        self.insert_chunk("chunk_4kb", &chunks_4k)?;
        self.insert_chunk("chunk_16kb", &chunks_16k)?;
        self.insert_chunk("chunk_64kb", &chunks_64k)?;
        self.insert_chunk("chunk_256kb", &chunks_256k)?;
        self.insert_chunk("chunk_1024kb", &chunks_1024k)?;
        Ok(())
    }

    fn split_chunk<'a>(
        &self,
        chunk_data: &[u8],
        granule_kb: usize, // 4, 16, 64, 256, 1024
        flag: u32,
        blob_id: &'a str,
    ) -> Result<Vec<(String, u32, &'a str, u32)>> {
        let granule_size = granule_kb * 1024;
        let mut chunks = Vec::new();
        let mut offset = 0;
        while offset < chunk_data.len() {
            let end = (offset + granule_size).min(chunk_data.len());
            let sub_chunk = &chunk_data[offset..end];
            let chunk_hash = RafsDigest::from_buf(sub_chunk, self.digest).to_string();
            let chunk_size = sub_chunk.len() as u32;
            chunks.push((chunk_hash, chunk_size, blob_id, flag));
            offset += granule_size;
        }

        Ok(chunks)
    }

    fn insert_chunk(
        &mut self,
        table_name: &str,
        chunks: &[(String, u32, &str, u32)],
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut blob_exists_sql = tx.prepare("SELECT 1 FROM blob WHERE hash = ?1")?;
            let update_sql = format!(
                "UPDATE {} SET count = count + 1 WHERE hash = ?1",
                table_name
            );
            let insert_sql = format!(
                "INSERT INTO {} (hash, size, count, flag) VALUES (?1, ?2, 1, ?3)",
                table_name
            );

            for (chunk_hash, chunk_size, blob_id, flag) in chunks {
                let blob_exists = blob_exists_sql.exists(params![*blob_id])?;
                if blob_exists {
                    continue;
                }
                let updated = tx.execute(&update_sql, params![chunk_hash])?;
                if updated == 0 {
                    tx.execute(&insert_sql, params![chunk_hash, *chunk_size as i64, flag])?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn insert_blobs(&mut self, blob_ids: &[String]) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let insert_sql = "INSERT OR IGNORE INTO blob (hash) VALUES (?1)";
            for blob_id in blob_ids {
                tx.execute(insert_sql, params![blob_id])?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}
