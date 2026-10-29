fn main() {
    println!("cargo:rerun-if-env-changed=PRISM_BUILD_ENVIRONMENT");
    if let Ok(environment) = std::env::var("PRISM_BUILD_ENVIRONMENT") {
        println!("cargo:rustc-env=PRISM_BUILD_ENVIRONMENT={environment}");
    }
    println!("cargo:rerun-if-env-changed=PRISM_BUILD_PANEL_URL");
    println!("cargo:rerun-if-env-changed=PRISM_BUILD_PRODUCT_NAME");
    println!("cargo:rerun-if-env-changed=PRISM_BUILD_UPDATER_ENABLED");
    if let Ok(panel_url) = std::env::var("PRISM_BUILD_PANEL_URL") {
        println!("cargo:rustc-env=PRISM_BUILD_PANEL_URL={panel_url}");
    }
    if let Ok(product_name) = std::env::var("PRISM_BUILD_PRODUCT_NAME") {
        println!("cargo:rustc-env=PRISM_BUILD_PRODUCT_NAME={product_name}");
    }
    if let Ok(updater_enabled) = std::env::var("PRISM_BUILD_UPDATER_ENABLED") {
        println!("cargo:rustc-env=PRISM_BUILD_UPDATER_ENABLED={updater_enabled}");
    }
    tauri_build::build()
}
