//! Semantic search on the device: a pinned multilingual embedding model
//! runs locally, so remembering by meaning sends nothing anywhere. The
//! model is downloaded on request through the device's proxy and checked
//! against pinned digests; a background worker embeds queued Objects and
//! installs a vector only if its Object was not queued again meanwhile.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use elri_proto::elri::v1::search::{SemanticState, SemanticStatus};
use sha2::{Digest as _, Sha256};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::model::ObjectId;
use crate::net::Net;
use crate::store::semantic::Pending;
use crate::store::Store;

/// Turns texts into unit vectors.
pub trait Embedder: Send + Sync {
    /// Model and revision; vectors of different models are never mixed.
    fn model(&self) -> &str;
    /// Embeds `texts`; `query` marks search text as opposed to stored text.
    fn embed(&self, texts: &[String], query: bool) -> Result<Vec<Vec<f32>>, String>;
}

fn normalize(v: &mut [f32]) {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
}

/// A model-free embedder: hashed words and character trigrams. It finds
/// shared words and word forms, not meaning; it exists for tests and as
/// a stand-in where no model can run.
#[derive(Debug, Clone, Copy, Default)]
pub struct HashingEmbedder;

const HASH_DIM: usize = 256;

fn fnv(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

impl Embedder for HashingEmbedder {
    #[allow(clippy::unnecessary_literal_bound)]
    fn model(&self) -> &str {
        "hashing-v1"
    }

    fn embed(&self, texts: &[String], _query: bool) -> Result<Vec<Vec<f32>>, String> {
        Ok(texts
            .iter()
            .map(|t| {
                let mut v = vec![0f32; HASH_DIM];
                let lower = t.to_lowercase();
                for word in lower.split(|c: char| !c.is_alphanumeric()) {
                    if word.is_empty() {
                        continue;
                    }
                    let idx = usize::try_from(fnv(word) % HASH_DIM as u64).unwrap_or(0);
                    v[idx] += 1.0;
                    let chars: Vec<char> = format!(" {word} ").chars().collect();
                    for w in chars.windows(3) {
                        let g: String = w.iter().collect();
                        let idx = usize::try_from(fnv(&g) % HASH_DIM as u64).unwrap_or(0);
                        v[idx] += 0.5;
                    }
                }
                normalize(&mut v);
                v
            })
            .collect())
    }
}

/// One file of a pinned model.
#[derive(Debug, Clone, Copy)]
pub struct ModelFile {
    /// File name in the repository.
    pub name: &'static str,
    /// Hex SHA-256.
    pub sha256: &'static str,
    /// Size in bytes.
    pub size: u64,
}

/// A model pinned by repository revision and file digests.
#[derive(Debug, Clone, Copy)]
pub struct ModelSpec {
    /// Hugging Face repository.
    pub repo: &'static str,
    /// Commit the files are taken from.
    pub revision: &'static str,
    /// The files.
    pub files: &'static [ModelFile],
    /// Directory name under `models/`.
    pub dir: &'static str,
}

/// The multilingual model used on every platform for now: Russian and
/// English among a hundred languages, 384 dimensions.
pub const E5_SMALL: ModelSpec = ModelSpec {
    repo: "intfloat/multilingual-e5-small",
    revision: "614241f622f53c4eeff9890bdc4f31cfecc418b3",
    files: &[
        ModelFile {
            name: "config.json",
            sha256: "69137736cab8b8903a07fe8afaafdda25aac55415a12a55d1bffa9f581abf959",
            size: 655,
        },
        ModelFile {
            name: "tokenizer.json",
            sha256: "0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39",
            size: 17_082_730,
        },
        ModelFile {
            name: "model.safetensors",
            sha256: "1a55775f53449dac10a2bcbc312469fac40b96d53198c407081a831f81c98477",
            size: 470_641_600,
        },
    ],
    dir: "multilingual-e5-small-614241f",
};

impl ModelSpec {
    /// `repo@revision-prefix`, the id vectors are stored under.
    pub fn id(&self) -> String {
        format!("{}@{}", self.repo, &self.revision[..7])
    }

    /// Where the files live under a data directory.
    pub fn path(&self, data_dir: &Path) -> PathBuf {
        data_dir.join("models").join(self.dir)
    }

    /// Whether every file is in place at its pinned size.
    pub fn present(&self, dir: &Path) -> bool {
        self.files
            .iter()
            .all(|f| std::fs::metadata(dir.join(f.name)).is_ok_and(|m| m.len() == f.size))
    }

    fn total(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }
}

/// Downloads the files of `spec` that are missing into `dir`, each to a
/// temporary file renamed into place only after its digest matched.
pub async fn download(
    net: &Net,
    spec: &ModelSpec,
    dir: &Path,
    progress: &(dyn Fn(u64, u64) + Send + Sync),
) -> Result<(), String> {
    use futures_util::StreamExt as _;
    use tokio::io::AsyncWriteExt as _;
    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|e| format!("{}: {e}", dir.display()))?;
    let total = spec.total();
    let mut done = 0u64;
    for f in spec.files {
        let target = dir.join(f.name);
        if std::fs::metadata(&target).is_ok_and(|m| m.len() == f.size) {
            done += f.size;
            progress(done, total);
            continue;
        }
        let url = format!(
            "https://huggingface.co/{}/resolve/{}/{}",
            spec.repo, spec.revision, f.name
        );
        let resp = net
            .client()
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("{}: {}", f.name, e.without_url()))?;
        if !resp.status().is_success() {
            return Err(format!("{}: HTTP {}", f.name, resp.status()));
        }
        let part = dir.join(format!("{}.part", f.name));
        let mut out = tokio::fs::File::create(&part)
            .await
            .map_err(|e| format!("{}: {e}", part.display()))?;
        let mut hash = Sha256::new();
        let mut size = 0u64;
        let mut body = resp.bytes_stream();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| format!("{}: {}", f.name, e.without_url()))?;
            size += chunk.len() as u64;
            if size > f.size {
                let _ = tokio::fs::remove_file(&part).await;
                return Err(format!("{}: larger than pinned", f.name));
            }
            hash.update(&chunk);
            out.write_all(&chunk)
                .await
                .map_err(|e| format!("{}: {e}", part.display()))?;
            progress(done + size, total);
        }
        out.flush().await.map_err(|e| e.to_string())?;
        drop(out);
        let digest = hash
            .finalize()
            .iter()
            .fold(String::with_capacity(64), |mut s, b| {
                use std::fmt::Write as _;
                let _ = write!(s, "{b:02x}");
                s
            });
        if digest != f.sha256 || size != f.size {
            let _ = tokio::fs::remove_file(&part).await;
            return Err(format!("{}: digest does not match the pinned one", f.name));
        }
        tokio::fs::rename(&part, &target)
            .await
            .map_err(|e| format!("{}: {e}", target.display()))?;
        done += f.size;
        progress(done, total);
    }
    Ok(())
}

/// The E5 model running on the CPU.
pub struct E5 {
    id: String,
    model: candle_transformers::models::bert::BertModel,
    tokenizer: tokenizers::Tokenizer,
}

/// Tokens of a text that are embedded.
const MAX_TOKENS: usize = 256;

impl E5 {
    /// Loads the model files from `dir`.
    pub fn load(spec: &ModelSpec, dir: &Path) -> Result<Self, String> {
        use candle_transformers::models::bert::{BertModel, Config, DTYPE};
        let config: Config = serde_json::from_slice(
            &std::fs::read(dir.join("config.json")).map_err(|e| format!("config: {e}"))?,
        )
        .map_err(|e| format!("config: {e}"))?;
        let mut tokenizer = tokenizers::Tokenizer::from_file(dir.join("tokenizer.json"))
            .map_err(|e| format!("tokenizer: {e}"))?;
        tokenizer
            .with_truncation(Some(tokenizers::TruncationParams {
                max_length: MAX_TOKENS,
                ..Default::default()
            }))
            .map_err(|e| format!("tokenizer: {e}"))?;
        tokenizer.with_padding(Some(tokenizers::PaddingParams {
            strategy: tokenizers::PaddingStrategy::BatchLongest,
            ..Default::default()
        }));
        let weights =
            std::fs::read(dir.join("model.safetensors")).map_err(|e| format!("weights: {e}"))?;
        let device = candle_core::Device::Cpu;
        let vb = candle_nn::VarBuilder::from_buffered_safetensors(weights, DTYPE, &device)
            .map_err(|e| format!("weights: {e}"))?;
        let model = BertModel::load(vb, &config).map_err(|e| format!("model: {e}"))?;
        Ok(Self {
            id: spec.id(),
            model,
            tokenizer,
        })
    }

    fn forward(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, candle_core::Error> {
        use candle_core::{DType, Tensor, D};
        let enc = self
            .tokenizer
            .encode_batch(texts, true)
            .map_err(|e| candle_core::Error::Msg(e.to_string()))?;
        let device = &self.model.device;
        let ids: Vec<Vec<u32>> = enc.iter().map(|e| e.get_ids().to_vec()).collect();
        let mask: Vec<Vec<u32>> = enc
            .iter()
            .map(|e| e.get_attention_mask().to_vec())
            .collect();
        let input = Tensor::new(ids, device)?;
        let mask = Tensor::new(mask, device)?;
        let types = input.zeros_like()?;
        let out = self.model.forward(&input, &types, Some(&mask))?;
        // Mean over the real tokens, then unit length.
        let m = mask.to_dtype(DType::F32)?.unsqueeze(2)?;
        let summed = out.broadcast_mul(&m)?.sum(1)?;
        let counts = m.sum(1)?;
        let mean = summed.broadcast_div(&counts)?;
        let norm = mean.sqr()?.sum_keepdim(D::Minus1)?.sqrt()?;
        mean.broadcast_div(&norm)?.to_vec2::<f32>()
    }
}

impl Embedder for E5 {
    fn model(&self) -> &str {
        &self.id
    }

    fn embed(&self, texts: &[String], query: bool) -> Result<Vec<Vec<f32>>, String> {
        let prefix = if query { "query: " } else { "passage: " };
        let texts: Vec<String> = texts.iter().map(|t| format!("{prefix}{t}")).collect();
        self.forward(texts).map_err(|e| e.to_string())
    }
}

/// Which embedder the core uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Backend {
    /// The pinned model, downloaded on request.
    #[default]
    Model,
    /// The hashing embedder, ready at once; for tests.
    Hashing,
}

#[derive(Debug, Clone)]
struct Progress {
    state: SemanticState,
    done: u64,
    total: u64,
    error: String,
}

/// The device's semantic engine.
pub struct Semantic {
    store: Arc<Store>,
    net: Arc<Net>,
    data_dir: PathBuf,
    backend: Backend,
    spec: ModelSpec,
    progress: Mutex<Progress>,
    embedder: RwLock<Option<Arc<dyn Embedder>>>,
    wake: Notify,
}

/// Objects embedded per batch.
const BATCH: usize = 16;

impl Semantic {
    /// The engine; nothing runs until [`Semantic::spawn`].
    pub fn new(store: Arc<Store>, net: Arc<Net>, data_dir: &Path, backend: Backend) -> Arc<Self> {
        Arc::new(Self {
            store,
            net,
            data_dir: data_dir.to_path_buf(),
            backend,
            spec: E5_SMALL,
            progress: Mutex::new(Progress {
                state: SemanticState::Missing,
                done: 0,
                total: E5_SMALL.total(),
                error: String::new(),
            }),
            embedder: RwLock::new(None),
            wake: Notify::new(),
        })
    }

    fn set(&self, state: SemanticState, error: &str) {
        let mut p = self
            .progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        p.state = state;
        error.clone_into(&mut p.error);
    }

    fn state(&self) -> SemanticState {
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .state
    }

    /// The model id vectors are stored under.
    pub fn model_id(&self) -> String {
        match self.backend {
            Backend::Model => self.spec.id(),
            Backend::Hashing => HashingEmbedder.model().to_owned(),
        }
    }

    /// The loaded embedder, if ready.
    pub fn embedder(&self) -> Option<Arc<dyn Embedder>> {
        self.embedder
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    async fn enabled(&self) -> bool {
        self.store
            .settings()
            .await
            .ok()
            .and_then(|s| s.semantic)
            .is_none_or(|s| s.enabled)
    }

    /// State, model and progress; `pending` is filled in by the caller.
    pub async fn status(&self) -> SemanticStatus {
        let mut p = self
            .progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if !self.enabled().await {
            p.state = SemanticState::Off;
        }
        SemanticStatus {
            state: p.state.into(),
            model: self.model_id(),
            bytes_done: i64::try_from(p.done).unwrap_or(i64::MAX),
            bytes_total: i64::try_from(p.total).unwrap_or(i64::MAX),
            error: p.error,
            pending: 0,
        }
    }

    /// Loads the embedder from files already in place, off the runtime.
    async fn load(self: &Arc<Self>) {
        if self.embedder().is_some() {
            return;
        }
        match self.backend {
            Backend::Hashing => {
                *self
                    .embedder
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some(Arc::new(HashingEmbedder));
                self.set(SemanticState::Ready, "");
            }
            Backend::Model => {
                self.set(SemanticState::Loading, "");
                let spec = self.spec;
                let dir = spec.path(&self.data_dir);
                match tokio::task::spawn_blocking(move || E5::load(&spec, &dir)).await {
                    Ok(Ok(m)) => {
                        *self
                            .embedder
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(m));
                        self.set(SemanticState::Ready, "");
                    }
                    Ok(Err(e)) => self.set(SemanticState::Failed, &e),
                    Err(e) => self.set(SemanticState::Failed, &e.to_string()),
                }
            }
        }
        self.wake.notify_one();
    }

    /// Downloads the model when missing and loads it, in the background.
    pub async fn prepare(self: &Arc<Self>) -> SemanticStatus {
        let state = self.state();
        let busy = matches!(
            state,
            SemanticState::Downloading | SemanticState::Loading | SemanticState::Ready
        );
        if !busy {
            let me = Arc::clone(self);
            tokio::spawn(async move {
                if me.backend == Backend::Model {
                    let dir = me.spec.path(&me.data_dir);
                    if !me.spec.present(&dir) {
                        me.set(SemanticState::Downloading, "");
                        let progress_of = Arc::clone(&me);
                        let progress = move |done: u64, total: u64| {
                            let mut p = progress_of
                                .progress
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            p.done = done;
                            p.total = total;
                        };
                        if let Err(e) = download(&me.net, &me.spec, &dir, &progress).await {
                            tracing::warn!(error = %e, "model download failed");
                            me.set(SemanticState::Failed, &e);
                            return;
                        }
                    }
                }
                me.load().await;
            });
        }
        self.status().await
    }

    /// Starts the engine: loads a model already on disk, and runs the
    /// worker that embeds queued Objects.
    pub fn spawn(self: &Arc<Self>) -> JoinHandle<()> {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let on_disk =
                me.backend == Backend::Hashing || me.spec.present(&me.spec.path(&me.data_dir));
            if on_disk && me.enabled().await {
                me.load().await;
            }
            let mut events = me.store.watch();
            let mut primed: std::collections::HashSet<ObjectId> = std::collections::HashSet::new();
            loop {
                tokio::select! {
                    () = me.wake.notified() => {}
                    _ = events.recv() => {}
                    () = tokio::time::sleep(Duration::from_secs(3)) => {}
                }
                if !me.enabled().await {
                    continue;
                }
                let Some(embedder) = me.embedder() else {
                    continue;
                };
                if let Err(e) = me.drain(&embedder, &mut primed).await {
                    tracing::warn!(error = %e, "semantic indexing failed");
                }
            }
        })
    }

    async fn drain(
        &self,
        embedder: &Arc<dyn Embedder>,
        primed: &mut std::collections::HashSet<ObjectId>,
    ) -> Result<(), String> {
        let model = embedder.model().to_owned();
        let spaces = self.store.list_spaces().await.map_err(|e| e.to_string())?;
        for info in spaces {
            let Ok(id) = ObjectId::parse(&info.id) else {
                continue;
            };
            let Ok(space) = self.store.space(id).await else {
                continue;
            };
            if primed.insert(id) {
                space
                    .enqueue_missing(&model)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            loop {
                let batch = space
                    .embed_backlog(&model, BATCH)
                    .await
                    .map_err(|e| e.to_string())?;
                if batch.is_empty() {
                    break;
                }
                let todo: Vec<String> = batch
                    .iter()
                    .filter(|p| !p.current && !p.text.is_empty())
                    .map(|p| p.text.clone())
                    .collect();
                let vectors = if todo.is_empty() {
                    Vec::new()
                } else {
                    let e = Arc::clone(embedder);
                    tokio::task::spawn_blocking(move || e.embed(&todo, false))
                        .await
                        .map_err(|e| e.to_string())??
                };
                let mut it = vectors.into_iter();
                let items: Vec<(Pending, Option<Vec<f32>>)> = batch
                    .into_iter()
                    .map(|p| {
                        let v = if !p.current && !p.text.is_empty() {
                            it.next()
                        } else {
                            None
                        };
                        (p, v)
                    })
                    .collect();
                let settled = space
                    .install_embeddings(&model, &items)
                    .await
                    .map_err(|e| e.to_string())?;
                if settled == 0 {
                    // Everything was queued again while it was embedded;
                    // the next wake-up takes it.
                    break;
                }
            }
        }
        Ok(())
    }

    /// Embeds a search text with the loaded model.
    pub async fn embed_query(&self, text: &str) -> Result<(String, Vec<f32>), String> {
        let Some(e) = self.embedder() else {
            return Err("the semantic model is not ready".to_owned());
        };
        let text = text.to_owned();
        let model = e.model().to_owned();
        let mut v = tokio::task::spawn_blocking(move || e.embed(&[text], true))
            .await
            .map_err(|e| e.to_string())??;
        Ok((model, v.pop().unwrap_or_default()))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    #[test]
    fn hashing_vectors_are_unit_and_close_for_shared_words() {
        let e = HashingEmbedder;
        let v = e
            .embed(
                &[
                    "graphs when planning studies".to_owned(),
                    "planning my studies with graphs".to_owned(),
                    "recipe for borscht".to_owned(),
                ],
                false,
            )
            .expect("embed");
        let dot = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
        assert!((dot(&v[0], &v[0]) - 1.0).abs() < 1e-5);
        assert!(dot(&v[0], &v[1]) > dot(&v[0], &v[2]));
    }

    /// Runs the real model when `ELRI_TEST_MODEL_DIR` points at its files.
    #[test]
    fn the_pinned_model_ranks_meaning_across_languages() {
        let Ok(dir) = std::env::var("ELRI_TEST_MODEL_DIR") else {
            return;
        };
        let m = E5::load(&E5_SMALL, Path::new(&dir)).expect("load");
        let docs = m
            .embed(
                &[
                    "Идеи о графах при планировании учёбы".to_owned(),
                    "Рецепт борща со свёклой".to_owned(),
                    "Workout log: squats and deadlifts".to_owned(),
                ],
                false,
            )
            .expect("embed");
        let q = m
            .embed(&["graphs for study planning".to_owned()], true)
            .expect("query")
            .pop()
            .expect("vector");
        assert_eq!(q.len(), 384);
        let dot = |a: &[f32]| a.iter().zip(&q).map(|(x, y)| x * y).sum::<f32>();
        assert!(dot(&docs[0]) > dot(&docs[1]), "graphs beat borscht");
        assert!(dot(&docs[0]) > dot(&docs[2]), "graphs beat workouts");
    }
}
