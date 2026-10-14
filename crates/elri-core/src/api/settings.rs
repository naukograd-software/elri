//! `SettingsService`: device settings, applied as they change.

use std::sync::Arc;

use elri_proto::elri::v1::settings::settings_service_ws::{
    register_settings_service_arc, SettingsServiceHandler,
};
use elri_proto::elri::v1::settings::{
    GetSettingsRequest, GetSettingsResponse, ProxyMode, ResolveProxyRequest, ResolveProxyResponse,
    UpdateSettingsRequest, UpdateSettingsResponse,
};
use ws_proto::{Request, Response, Server, Status};

use super::{invalid, Services};

pub(super) fn register(server: &Server, services: Arc<Services>) {
    register_settings_service_arc(server, services);
}

impl SettingsServiceHandler for Services {
    async fn get_settings(
        &self,
        _req: Request<GetSettingsRequest>,
    ) -> Result<Response<GetSettingsResponse>, Status> {
        Ok(Response::new(GetSettingsResponse {
            settings: Some(self.store.settings().await?),
        }))
    }

    async fn update_settings(
        &self,
        req: Request<UpdateSettingsRequest>,
    ) -> Result<Response<UpdateSettingsResponse>, Status> {
        let m = req.msg;
        let mut settings = self.store.settings().await?;
        if let Some(proxy) = m.proxy {
            if proxy.mode() == ProxyMode::Manual && proxy.url.trim().is_empty() {
                return Err(invalid("a manual proxy needs a url"));
            }
            // Applied first: a URL the client cannot use is refused and
            // the stored settings stay as they were.
            self.net
                .reconfigure(proxy.clone())
                .map_err(|e| invalid(e.to_string()))?;
            settings.proxy = Some(proxy);
        }
        if let Some(semantic) = m.semantic {
            settings.semantic = Some(semantic);
        }
        self.store.set_settings(&settings).await?;
        Ok(Response::new(UpdateSettingsResponse {
            settings: Some(settings),
        }))
    }

    async fn resolve_proxy(
        &self,
        req: Request<ResolveProxyRequest>,
    ) -> Result<Response<ResolveProxyResponse>, Status> {
        let url = if req.msg.url.trim().is_empty() {
            "https://api.openai.com/".to_owned()
        } else {
            req.msg.url.trim().to_owned()
        };
        let (proxy, source) = self.net.resolve(&url);
        Ok(Response::new(ResolveProxyResponse { proxy, source }))
    }
}
