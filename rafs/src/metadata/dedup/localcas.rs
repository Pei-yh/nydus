use std::os::unix::net::UnixStream;
use std::sync::Arc;
use r2d2::{Pool, PooledConnection, ManageConnection};

pub struct CasDb {
    pool: Pool<UnixSocketManager>,
}

impl CasDb {
    pub fn new() -> Result<CasDb> {
        let socket = "/tmp/cas.sock";
        let manager = UnixSocketManager::new(socket_path)?;
        let pool = r2d2::Pool::builder().max_size(10).build(manager)?;
        Ok(CasDb { pool })
    }

    pub fn get_connection(&self) -> Result<PooledConnection<UnixSocketManager>, Box<dyn Error>> {
        let conn = self.pool.get()?;
        Ok(conn)
    }

    pub fn get_chunk_info(&self, chunk_id: &str) -> Result<Option<(String, u64)>> {
        let mut conn: PooledConnection<UnixSocketManager> = self.get_connection()?;
        let request = format!("GET {}\n", chunk_id);
        conn.write_all(request.as_bytes())?;
        let mut response = String::new();
        conn.read_to_string(&mut response)?;
        if response.trim().is_empty() {
            Ok(None)
        } else {
            Ok(Some(response.trim().to_string()))
        }
    }
}

struct UnixSocketManager {
    socket: Arc<String>,
}

impl UnixSocketManager {
    pub fn new(path: &str) -> Result<UnixSocketManager> {
        Ok(UnixSocketManager {
            socket: Arc::new(path.to_string()),
        })
    }
}

impl ManageConnection for UnixSocketManager {
    type Connection = UnixStream;
    type Error = Box<dyn Error>;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let socket = UnixStream::connect(&self.socket)?;
        Ok(socket)
    }

    fn is_valid(&self, _conn: &Self::Connection) -> Result<(), Self::Error> {
        Ok(())
    }

    fn has_broken(&self, _conn: &Self::Connection) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn test_socket_connection() -> Result<()> {
        let db = CasDb::new()?;

        let mut conn = db.pool.get()?;

        let msg = b"Hello, Unix Socket!";
        conn.write_all(msg)?;

        let mut buffer = [0; 1024];
        let size = conn.read(&mut buffer)?;
        println!("Received: {}", String::from_utf8_lossy(&buffer[..size]));

        Ok(())
    }
}