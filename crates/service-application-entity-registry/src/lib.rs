//! Application Entity registry for DICOM services.
//!
//! This crate defines local and peer Application Entities and registry traits
//! for resolving them by AE title. The default concrete implementation is
//! [`InMemoryApplicationEntityRegistry`], which keeps local and peer AE titles
//! unique inside the current process.
//!
//! [`ApplicationEntityRegistry`] is the read interface intended for runtime
//! service dependencies. [`ApplicationEntityRegistryWriter`] is the write
//! interface for bootstrap, configuration, or future administration flows.
//!
//! With the `grpc` feature enabled, this crate also exposes tonic client and
//! server adapters for the read and write interfaces. The gRPC wire
//! definitions are owned by this crate and built with vendored `protoc`.

mod application_entity;
#[cfg(feature = "grpc")]
mod grpc;
mod registry;

pub use application_entity::{
    AeTitle, AeTitleError, ApplicationEntityError, LocalApplicationEntity, PeerApplicationEntity,
};
#[cfg(any(test, feature = "test-support"))]
pub use application_entity::{local_ae, peer_ae};
#[cfg(feature = "grpc")]
pub use grpc::{
    ApplicationEntityRegistryGrpcService, ApplicationEntityRegistryService,
    ApplicationEntityRegistryServiceClient, ApplicationEntityRegistryServiceServer,
    ApplicationEntityRegistryWriterGrpcService, ApplicationEntityRegistryWriterService,
    ApplicationEntityRegistryWriterServiceClient, ApplicationEntityRegistryWriterServiceServer,
    GrpcApplicationEntityRegistryClient, GrpcApplicationEntityRegistryWriterClient,
};
pub use registry::{
    ApplicationEntityRegistry, ApplicationEntityRegistryError, ApplicationEntityRegistryWriter,
    InMemoryApplicationEntityRegistry,
};
