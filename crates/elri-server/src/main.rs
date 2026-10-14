//! `elri-server`: serve, or administer users without serving.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use elri_server::store::ServerStore;
use elri_server::{Config, Server};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Directory holding the server database and Space files.
    #[arg(
        long,
        env = "ELRI_SERVER_DATA_DIR",
        default_value = "./elri-server-data"
    )]
    data_dir: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Run the server.
    Serve {
        /// Address to listen on.
        #[arg(long, default_value = "127.0.0.1:8710")]
        bind: SocketAddr,
    },
    /// Manage users.
    User {
        #[command(subcommand)]
        cmd: UserCmd,
    },
}

#[derive(Subcommand, Debug)]
enum UserCmd {
    /// Create a user and print its token once.
    Add {
        /// Unique user name.
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    match args.cmd {
        Cmd::Serve { bind } => {
            let server = Server::start(Config {
                data_dir: args.data_dir,
                bind,
            })
            .await?;
            println!("listening on {}", server.url());
            tokio::signal::ctrl_c().await?;
            server.shutdown();
            Ok(())
        }
        Cmd::User { cmd } => match cmd {
            UserCmd::Add { name } => {
                let registry = ServerStore::open(&args.data_dir).await?;
                let (user, token) = registry.create_user(&name).await?;
                println!("user {} ({})", user.name, user.id);
                println!("token {token}");
                Ok(())
            }
        },
    }
}
