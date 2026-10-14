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
use crate::net::{split_password, with_password, PROXY_PASSWORD_KEY};

fn secret_status(e: &crate::secrets::SecretsError) -> Status {
    tracing::error!(error = %e, "credential store failed");
    Status::new(ws_proto::Code::Internal, "credential store failed")
}

pub(super) fn register(server: &Server, services: Arc<Services>) {
    register_settings_service_arc(server, services);
}

impl SettingsServiceHandler for Services {
    async fn get_settings(
        &self,
        _req: Request<GetSettingsRequest>,
    ) -> Result<Response<GetSettingsResponse>, Status> {
        let mut settings = self.store.settings().await?;
        // The stored URL carries no password; show that one is kept.
        if let Some(proxy) = settings.proxy.as_mut() {
            if self
                .secrets
                .get(PROXY_PASSWORD_KEY)
                .await
                .map_err(|e| secret_status(&e))?
                .is_some()
            {
                proxy.url = with_password(&proxy.url, "***");
            }
        }
        Ok(Response::new(GetSettingsResponse {
            settings: Some(settings),
        }))
    }

    async fn update_settings(
        &self,
        req: Request<UpdateSettingsRequest>,
    ) -> Result<Response<UpdateSettingsResponse>, Status> {
        let m = req.msg;
        let mut settings = self.store.settings().await?;
        if let Some(mut proxy) = m.proxy {
            if proxy.mode() == ProxyMode::Manual && proxy.url.trim().is_empty() {
                return Err(invalid("a manual proxy needs a url"));
            }
            // The password goes to the credential store, never into the
            // settings record. `***` is the password shown back: keep it.
            let (bare, password) = split_password(proxy.url.trim());
            let kept = self
                .secrets
                .get(PROXY_PASSWORD_KEY)
                .await
                .map_err(|e| secret_status(&e))?;
            let password = match password.as_deref() {
                Some("***") => kept,
                Some(p) => Some(p.to_owned()),
                None => None,
            };
            let mut live = proxy.clone();
            live.url = match &password {
                Some(p) => with_password(&bare, p),
                None => bare.clone(),
            };
            // Applied first: a URL the client cannot use is refused and
            // the stored settings stay as they were.
            self.net
                .reconfigure(live)
                .map_err(|e| invalid(e.to_string()))?;
            match &password {
                Some(p) => self.secrets.set(PROXY_PASSWORD_KEY, p).await,
                None => self.secrets.delete(PROXY_PASSWORD_KEY).await,
            }
            .map_err(|e| secret_status(&e))?;
            proxy.url = bare;
            settings.proxy = Some(proxy);
        }
        if let Some(semantic) = m.semantic {
            settings.semantic = Some(semantic);
        }
        if let Some(ai) = m.ai {
            for id in &ai.authorized_space_ids {
                crate::model::ObjectId::parse(id)
                    .map_err(|_| invalid("ai: authorized spaces are space ids"))?;
            }
            settings.ai = Some(ai);
        }
        self.store.set_settings(&settings).await?;
        if let Some(proxy) = settings.proxy.as_mut() {
            if self
                .secrets
                .get(PROXY_PASSWORD_KEY)
                .await
                .map_err(|e| secret_status(&e))?
                .is_some()
            {
                proxy.url = with_password(&proxy.url, "***");
            }
        }
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
