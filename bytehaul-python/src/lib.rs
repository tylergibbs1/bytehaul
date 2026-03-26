use std::sync::Mutex;
use std::time::Instant;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use bytehaul_lib::client::Client as InnerClient;
use bytehaul_lib::config::TransferConfig;

const VERSION: &str = "0.2.0";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a new single-threaded Tokio runtime for blocking into async code.
fn runtime() -> Result<tokio::runtime::Runtime, PyErr> {
    tokio::runtime::Runtime::new()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {e}")))
}

/// Convert an `anyhow::Error` into a Python `RuntimeError`.
fn to_pyerr(e: anyhow::Error) -> PyErr {
    PyRuntimeError::new_err(format!("{e:#}"))
}

/// Build a [`TransferConfig`] from the optional keyword arguments that Python
/// callers pass to `send`, `send_directory`, and `pull`.
fn config_from_kwargs(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<TransferConfig> {
    let mut builder = TransferConfig::builder();

    let Some(kwargs) = kwargs else {
        return Ok(builder.build());
    };

    if let Some(val) = kwargs.get_item("resume")? {
        builder = builder.resume(val.extract::<bool>()?);
    }
    if let Some(val) = kwargs.get_item("delta")? {
        builder = builder.delta(val.extract::<bool>()?);
    }
    if let Some(val) = kwargs.get_item("block_size_mb")? {
        builder = builder.block_size_mb(val.extract::<u32>()?);
    }
    if let Some(val) = kwargs.get_item("parallel")? {
        builder = builder.max_parallel_streams(val.extract::<usize>()?);
    }
    // `compress` is accepted for forward-compatibility but is not yet wired
    // to the engine (compression happens at the protocol layer in
    // bytehaul-proto and is always enabled when zstd support is compiled in).
    if let Some(val) = kwargs.get_item("compress")? {
        let _: bool = val.extract()?; // validate type, ignore value for now
    }

    Ok(builder.build())
}

// ---------------------------------------------------------------------------
// TransferResult
// ---------------------------------------------------------------------------

/// Result returned by transfer operations.
#[pyclass(frozen)]
#[derive(Clone, Debug)]
struct TransferResult {
    /// Wall-clock seconds the transfer took.
    #[pyo3(get)]
    elapsed_secs: f64,
    /// Total bytes transferred (0 when byte-level metrics are unavailable).
    #[pyo3(get)]
    total_bytes: u64,
    /// Transfer speed in megabits per second.
    #[pyo3(get)]
    speed_mbps: f64,
    /// Whether the transfer was integrity-verified (BLAKE3).
    #[pyo3(get)]
    verified: bool,
}

#[pymethods]
impl TransferResult {
    fn __repr__(&self) -> String {
        format!(
            "TransferResult(elapsed_secs={:.2}, total_bytes={}, speed_mbps={:.2}, verified={})",
            self.elapsed_secs, self.total_bytes, self.speed_mbps, self.verified,
        )
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// High-level ByteHaul client.
///
/// Create via the classmethods ``Client.connect_ssh(remote)`` or
/// ``Client.connect_daemon(addr)``, then call ``send()``,
/// ``send_directory()``, or ``pull()`` to transfer files.
///
/// Example::
///
///     from bytehaul import Client
///
///     client = Client.connect_ssh("user@host")
///     result = client.send("/local/data.bin", "/remote/data.bin")
///     print(result)
#[pyclass]
struct Client {
    /// The underlying Rust client.  Wrapped in a `Mutex` so that
    /// `with_config()` (which consumes `self`) can be used per-operation
    /// without requiring `&mut self` on the Python side.
    inner: Mutex<Option<InnerClient>>,
}

impl Client {
    /// Temporarily take the inner client, apply a config, run `op`, then
    /// put the client back (with default config restored).
    fn with_configured_client<F, T>(&self, config: TransferConfig, op: F) -> PyResult<T>
    where
        F: FnOnce(&InnerClient) -> Result<T, PyErr>,
    {
        let client = {
            let mut guard = self.inner.lock().map_err(|e| {
                PyRuntimeError::new_err(format!("Client lock poisoned: {e}"))
            })?;
            guard.take().ok_or_else(|| {
                PyRuntimeError::new_err("Client has been consumed or is in use")
            })?
        };

        // Apply per-call config.
        let configured = client.with_config(config);
        let result = op(&configured);

        // Restore the client with default config so it can be reused.
        let restored = configured.with_config(TransferConfig::default());
        let mut guard = self.inner.lock().map_err(|e| {
            PyRuntimeError::new_err(format!("Client lock poisoned: {e}"))
        })?;
        *guard = Some(restored);

        result
    }
}

#[pymethods]
impl Client {
    /// Connect to a remote host by bootstrapping a receiver via SSH.
    ///
    /// Args:
    ///     remote: SSH target, e.g. ``"user@host"`` or ``"host"``.
    ///
    /// Returns:
    ///     A connected ``Client`` instance.
    #[classmethod]
    fn connect_ssh(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
        remote: String,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            let rt = runtime()?;
            let inner = rt
                .block_on(InnerClient::connect_ssh(&remote))
                .map_err(to_pyerr)?;
            Ok(Self {
                inner: Mutex::new(Some(inner)),
            })
        })
    }

    /// Connect to a running ByteHaul daemon.
    ///
    /// Args:
    ///     addr: Socket address, e.g. ``"192.168.1.10:4242"``.
    ///
    /// Returns:
    ///     A connected ``Client`` instance.
    #[classmethod]
    fn connect_daemon(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
        addr: String,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            let rt = runtime()?;
            let inner = rt
                .block_on(InnerClient::connect_daemon(&addr, None))
                .map_err(to_pyerr)?;
            Ok(Self {
                inner: Mutex::new(Some(inner)),
            })
        })
    }

    /// Send a single file to the remote destination.
    ///
    /// Args:
    ///     source: Local file path.
    ///     dest: Remote destination path.
    ///
    /// Keyword Args:
    ///     resume (bool): Enable transfer resumption (default ``True``).
    ///     delta (bool): Only send changed blocks (default ``False``).
    ///     block_size_mb (int): Block size in megabytes.
    ///     parallel (int): Number of parallel QUIC streams.
    ///     compress (bool): Reserved for future use.
    ///
    /// Returns:
    ///     A ``TransferResult`` with timing information.
    #[pyo3(signature = (source, dest, **kwargs))]
    fn send(
        &self,
        py: Python<'_>,
        source: String,
        dest: String,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<TransferResult> {
        let config = config_from_kwargs(kwargs)?;

        py.allow_threads(|| {
            let start = Instant::now();
            self.with_configured_client(config, |client| {
                let rt = runtime()?;
                let transfer = rt
                    .block_on(client.send_file(&source, &dest))
                    .map_err(to_pyerr)?;
                rt.block_on(transfer.wait()).map_err(to_pyerr)?;
                Ok(())
            })?;
            let elapsed = start.elapsed().as_secs_f64();
            Ok(TransferResult {
                elapsed_secs: elapsed,
                total_bytes: 0,
                speed_mbps: 0.0,
                verified: true,
            })
        })
    }

    /// Send an entire directory to the remote destination.
    ///
    /// Accepts the same keyword arguments as ``send()``.
    #[pyo3(signature = (source, dest, **kwargs))]
    fn send_directory(
        &self,
        py: Python<'_>,
        source: String,
        dest: String,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<TransferResult> {
        let config = config_from_kwargs(kwargs)?;

        py.allow_threads(|| {
            let start = Instant::now();
            self.with_configured_client(config, |client| {
                let rt = runtime()?;
                let transfer = rt
                    .block_on(client.send_directory(&source, &dest))
                    .map_err(to_pyerr)?;
                rt.block_on(transfer.wait()).map_err(to_pyerr)?;
                Ok(())
            })?;
            let elapsed = start.elapsed().as_secs_f64();
            Ok(TransferResult {
                elapsed_secs: elapsed,
                total_bytes: 0,
                speed_mbps: 0.0,
                verified: true,
            })
        })
    }

    /// Pull a file from the remote to a local destination.
    ///
    /// Accepts the same keyword arguments as ``send()``.
    #[pyo3(signature = (remote_path, local_dest, **kwargs))]
    fn pull(
        &self,
        py: Python<'_>,
        remote_path: String,
        local_dest: String,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<TransferResult> {
        let config = config_from_kwargs(kwargs)?;

        py.allow_threads(|| {
            let start = Instant::now();
            self.with_configured_client(config, |client| {
                let rt = runtime()?;
                let transfer = rt
                    .block_on(client.pull_file(&remote_path, &local_dest))
                    .map_err(to_pyerr)?;
                rt.block_on(transfer.wait()).map_err(to_pyerr)?;
                Ok(())
            })?;
            let elapsed = start.elapsed().as_secs_f64();
            Ok(TransferResult {
                elapsed_secs: elapsed,
                total_bytes: 0,
                speed_mbps: 0.0,
                verified: true,
            })
        })
    }

    fn __repr__(&self) -> &'static str {
        "Client(connected)"
    }
}

// ---------------------------------------------------------------------------
// Module-level convenience function
// ---------------------------------------------------------------------------

/// Connect via SSH and send a file in one call.
///
/// This is a shorthand for creating a ``Client`` with ``connect_ssh`` and
/// immediately calling ``send()``.
///
/// Args:
///     remote: SSH target, e.g. ``"user@host"``.
///     source: Local file path.
///     dest: Remote destination path.
///
/// Keyword Args:
///     See ``Client.send()`` for supported keyword arguments.
///
/// Returns:
///     A ``TransferResult`` with timing information.
///
/// Example::
///
///     import bytehaul
///     result = bytehaul.send("user@host", "/local/file.tar", "/remote/file.tar")
#[pyfunction]
#[pyo3(signature = (remote, source, dest, **kwargs))]
fn send(
    py: Python<'_>,
    remote: String,
    source: String,
    dest: String,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<TransferResult> {
    let config = config_from_kwargs(kwargs)?;

    py.allow_threads(move || {
        let rt = runtime()?;
        let start = Instant::now();

        let client = rt
            .block_on(InnerClient::connect_ssh(&remote))
            .map_err(to_pyerr)?
            .with_config(config);

        let transfer = rt
            .block_on(client.send_file(&source, &dest))
            .map_err(to_pyerr)?;
        rt.block_on(transfer.wait()).map_err(to_pyerr)?;

        let elapsed = start.elapsed().as_secs_f64();
        Ok(TransferResult {
            elapsed_secs: elapsed,
            total_bytes: 0,
            speed_mbps: 0.0,
            verified: true,
        })
    })
}

// ---------------------------------------------------------------------------
// Python module definition
// ---------------------------------------------------------------------------

/// ByteHaul Python SDK — fast file transfer over QUIC.
///
/// Quick start::
///
///     from bytehaul import Client
///
///     client = Client.connect_ssh("user@host")
///     result = client.send("./data.bin", "/tmp/data.bin", parallel=32)
///     print(f"Transferred in {result.elapsed_secs:.1f}s")
#[pymodule]
fn bytehaul(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;
    m.add_class::<Client>()?;
    m.add_class::<TransferResult>()?;
    m.add_function(wrap_pyfunction!(send, m)?)?;
    Ok(())
}
