use crate::assertions::command_succeeded;
use crate::dcmtk::{self, StoreOptions};
use crate::harness::DimseEndpoint;

pub fn baseline(ctx: &impl DimseEndpoint) {
    store_fixture(ctx);
    repeat_store(ctx);
    ctx.wait_until_fixture_is_queryable();
}

pub fn association_and_transfer_parameters(ctx: &impl DimseEndpoint) {
    calling_ae_variation(ctx);
    transfer_syntax_proposals(ctx);
    pdu_size_variation(ctx);
    ctx.wait_until_fixture_is_queryable();
}

fn store_fixture(ctx: &impl DimseEndpoint) {
    let output = dcmtk::storescu(
        ctx.host(),
        ctx.port(),
        ctx.called_ae(),
        &ctx.fixture().path,
        &StoreOptions::default(),
    );
    command_succeeded("C-STORE stores fixture", &output);
}

fn repeat_store(ctx: &impl DimseEndpoint) {
    let output = dcmtk::storescu(
        ctx.host(),
        ctx.port(),
        ctx.called_ae(),
        &ctx.fixture().path,
        &StoreOptions::default(),
    );
    command_succeeded("C-STORE repeat stores same fixture", &output);
}

fn calling_ae_variation(ctx: &impl DimseEndpoint) {
    let options = StoreOptions {
        calling_ae: "ALTSTORE".to_string(),
        ..StoreOptions::default()
    };
    let output = dcmtk::storescu(
        ctx.host(),
        ctx.port(),
        ctx.called_ae(),
        &ctx.fixture().path,
        &options,
    );
    command_succeeded("C-STORE accepts alternate calling AE", &output);
}

fn transfer_syntax_proposals(ctx: &impl DimseEndpoint) {
    for (name, flag) in [
        ("default uncompressed", None),
        ("explicit little", Some("-xe")),
        ("implicit little", Some("-xi")),
        ("explicit big", Some("-xb")),
    ] {
        let options = StoreOptions {
            transfer_syntax_flag: flag,
            ..StoreOptions::default()
        };
        let output = dcmtk::storescu(
            ctx.host(),
            ctx.port(),
            ctx.called_ae(),
            &ctx.fixture().path,
            &options,
        );
        command_succeeded(&format!("C-STORE transfer syntax proposal {name}"), &output);
    }
}

fn pdu_size_variation(ctx: &impl DimseEndpoint) {
    let options = StoreOptions {
        max_pdu: Some(4096),
        ..StoreOptions::default()
    };
    let output = dcmtk::storescu(
        ctx.host(),
        ctx.port(),
        ctx.called_ae(),
        &ctx.fixture().path,
        &options,
    );
    command_succeeded("C-STORE accepts small max PDU", &output);
}
