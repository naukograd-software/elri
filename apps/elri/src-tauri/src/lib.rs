//! Tauri shell: starts the core in-process, hands its loopback endpoint to the
//! renderer, and keeps running in the tray after the window closes.

use std::sync::Mutex;

use elri_core::{BuildInfo, Config, Core, Endpoint};
use tauri::Manager;

/// Core handle kept for the lifetime of the application.
struct CoreState(Mutex<Option<Core>>);

/// The renderer asks for this once at start-up and then speaks ws-proto.
/// Tauri commands receive `State` by value; the lint does not apply here.
#[tauri::command]
#[allow(clippy::needless_pass_by_value)]
fn core_endpoint(state: tauri::State<'_, CoreState>) -> Result<Endpoint, String> {
    let guard = state
        .0
        .lock()
        .map_err(|_| "core state poisoned".to_owned())?;
    guard
        .as_ref()
        .map(|c| c.endpoint().clone())
        .ok_or_else(|| "core is not running".to_owned())
}

/// Entry point shared by the desktop binary and the Android library.
#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tauri::Builder::default()
        .manage(CoreState(Mutex::new(None)))
        .invoke_handler(tauri::generate_handler![core_endpoint])
        .setup(|app| {
            let data_dir = app.path().app_data_dir()?;
            let config = Config::new(
                data_dir,
                BuildInfo {
                    version: env!("CARGO_PKG_VERSION"),
                    commit: option_env!("ELRI_COMMIT").unwrap_or(""),
                },
            );
            let core = tauri::async_runtime::block_on(Core::start(config))?;
            if let Ok(mut guard) = app.state::<CoreState>().0.lock() {
                *guard = Some(core);
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "tauri failed");
            std::process::exit(1);
        });
}
