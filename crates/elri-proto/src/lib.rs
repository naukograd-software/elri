// @generated
pub mod elri {
    pub mod v1 {
        #[cfg(feature = "elri-v1-core")]
        // @@protoc_insertion_point(attribute:elri.v1.core)
        pub mod core {
            include!("elri/v1/core/elri.v1.core.rs");
            // @@protoc_insertion_point(elri.v1.core)
        }
        #[cfg(feature = "elri-v1-space")]
        // @@protoc_insertion_point(attribute:elri.v1.space)
        pub mod space {
            include!("elri/v1/space/elri.v1.space.rs");
            // @@protoc_insertion_point(elri.v1.space)
        }
    }
}
