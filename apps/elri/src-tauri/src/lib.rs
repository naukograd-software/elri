//! Tauri shell: starts the core in-process, hands its loopback endpoint to the
//! renderer, and keeps running in the tray after the window closes.

use std::sync::Mutex;

use elri_core::secrets::SecretsBackend;
use elri_core::{BuildInfo, Config, Core, Endpoint};
#[cfg(desktop)]
use tauri::menu::{Menu, MenuItem};
#[cfg(desktop)]
use tauri::tray::TrayIconBuilder;
use tauri::Manager;
#[cfg(desktop)]
use tauri::WindowEvent;

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

/// Opens the provider's sign-in page in the system browser. Only the
/// provider's own HTTPS pages are opened, so the renderer cannot use this
/// to launch anything else.
#[tauri::command]
#[allow(clippy::needless_pass_by_value)]
fn open_sign_in(app: tauri::AppHandle, url: String) -> Result<(), String> {
    use tauri_plugin_opener::OpenerExt as _;
    let ok = url.starts_with("https://auth.openai.com/") && !url.contains(char::is_whitespace);
    if !ok {
        return Err("only the provider's sign-in page can be opened".to_owned());
    }
    app.opener()
        .open_url(url, None::<&str>)
        .map_err(|e| e.to_string())
}

/// Entry point shared by the desktop binary and the Android library.
#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let builder = tauri::Builder::default();
    // Desktop: a second launch focuses the running instance instead of
    // fighting it for the data directory.
    #[cfg(desktop)]
    let builder = builder.plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
        if let Some(w) = app.get_webview_window("main") {
            let _ = w.show();
            let _ = w.set_focus();
        }
    }));
    builder
        .manage(CoreState(Mutex::new(None)))
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![core_endpoint, open_sign_in])
        .setup(|app| {
            let data_dir = app.path().app_data_dir()?;
            let mut config = Config::new(
                data_dir,
                BuildInfo {
                    version: env!("CARGO_PKG_VERSION"),
                    commit: option_env!("ELRI_COMMIT").unwrap_or(""),
                },
            );
            // The desktop keeps the server token in the OS credential store;
            // mobile builds have no store and fall back to the data directory.
            config.secrets = SecretsBackend::Auto;
            let core = match tauri::async_runtime::block_on(Core::start(config)) {
                Ok(core) => core,
                Err(e) => {
                    tracing::error!(error = %e, "core failed to start");
                    eprintln!("elri: {e}");
                    std::process::exit(2);
                }
            };
            if let Ok(mut guard) = app.state::<CoreState>().0.lock() {
                *guard = Some(core);
            }

            // Desktop: closing the window hides it; the core keeps running
            // until Quit is chosen from the tray.
            #[cfg(desktop)]
            {
                let open = MenuItem::with_id(app, "open", "Open elri", true, None::<&str>)?;
                let quit = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
                let menu = Menu::with_items(app, &[&open, &quit])?;
                let mut tray = TrayIconBuilder::new()
                    .menu(&menu)
                    .show_menu_on_left_click(true);
                if let Some(icon) = app.default_window_icon() {
                    tray = tray.icon(icon.clone());
                }
                tray.on_menu_event(|app, event| match event.id.as_ref() {
                    "open" => {
                        if let Some(w) = app.get_webview_window("main") {
                            let _ = w.show();
                            let _ = w.set_focus();
                        }
                    }
                    "quit" => {
                        if let Ok(mut guard) = app.state::<CoreState>().0.lock() {
                            if let Some(core) = guard.take() {
                                core.shutdown();
                            }
                        }
                        app.exit(0);
                    }
                    _ => {}
                })
                .build(app)?;
            }
            Ok(())
        })
        .on_window_event(|window, event| {
            #[cfg(desktop)]
            if let WindowEvent::CloseRequested { api, .. } = event {
                let _ = window.hide();
                api.prevent_close();
            }
            #[cfg(not(desktop))]
            {
                let _ = (window, event);
            }
        })
        .run(tauri::generate_context!())
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "tauri failed");
            std::process::exit(1);
        });
}
