//! Desktop entry point of the elri application.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    elri_app_lib::run();
}
