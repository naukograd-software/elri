//! Desktop entry point of the elri application.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    // WebKitGTK's DMA-BUF renderer fails on some Wayland setups with
    // "Error 71 (Protocol error) dispatching to Wayland display"; the
    // software path works everywhere. Must be set before GTK initializes.
    #[cfg(target_os = "linux")]
    if std::env::var_os("WEBKIT_DISABLE_DMABUF_RENDERER").is_none() {
        std::env::set_var("WEBKIT_DISABLE_DMABUF_RENDERER", "1");
    }
    elri_app_lib::run();
}
