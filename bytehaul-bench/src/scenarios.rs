use anyhow::Result;
use serde::{Deserialize, Serialize};

/// A benchmark scenario with specific network conditions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scenario {
    pub name: String,
    pub description: String,
    pub bandwidth_mbps: u64,
    pub rtt_ms: u64,
    pub loss_percent: f64,
    pub file_size_bytes: u64,
    pub file_count: u32,
    pub purpose: String,
}

impl Scenario {
    /// Generate tc/netem commands to simulate this scenario's network conditions.
    #[allow(dead_code)]
    pub fn netem_commands(&self, interface: &str) -> Vec<String> {
        let mut cmds = vec![
            format!("sudo tc qdisc del dev {} root 2>/dev/null || true", interface),
        ];

        let mut netem = format!(
            "sudo tc qdisc add dev {} root netem delay {}ms",
            interface,
            self.rtt_ms / 2 // one-way delay
        );

        if self.loss_percent > 0.0 {
            netem.push_str(&format!(" loss {}%", self.loss_percent));
        }

        cmds.push(netem);

        // Add bandwidth limit via tbf
        if self.bandwidth_mbps > 0 {
            let rate_kbit = self.bandwidth_mbps * 1000;
            let burst = (self.bandwidth_mbps * 1000 / 8).max(1); // burst in bytes
            cmds.push(format!(
                "sudo tc qdisc add dev {} parent 1:1 handle 10: tbf rate {}kbit burst {}k latency 50ms",
                interface, rate_kbit, burst / 1000
            ));
        }

        cmds
    }

    /// Generate cleanup tc commands.
    #[allow(dead_code)]
    pub fn netem_cleanup(interface: &str) -> String {
        format!("sudo tc qdisc del dev {} root 2>/dev/null || true", interface)
    }
}

/// Get all standard benchmark scenarios.
pub fn all_scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "lan-baseline".to_string(),
            description: "LAN baseline - ceiling test, CPU/IO bound".to_string(),
            bandwidth_mbps: 10_000,
            rtt_ms: 1,
            loss_percent: 0.0,
            file_size_bytes: 10 * 1024 * 1024 * 1024, // 10 GB
            file_count: 1,
            purpose: "Ceiling test, CPU/IO bound".to_string(),
        },
        Scenario {
            name: "metro-link".to_string(),
            description: "Metro link - moderate BDP".to_string(),
            bandwidth_mbps: 1_000,
            rtt_ms: 10,
            loss_percent: 0.0,
            file_size_bytes: 10 * 1024 * 1024 * 1024,
            file_count: 1,
            purpose: "Moderate BDP".to_string(),
        },
        Scenario {
            name: "cross-region".to_string(),
            description: "Cross-region - high BDP, primary win scenario".to_string(),
            bandwidth_mbps: 1_000,
            rtt_ms: 50,
            loss_percent: 0.0,
            file_size_bytes: 10 * 1024 * 1024 * 1024,
            file_count: 1,
            purpose: "High BDP, primary win scenario".to_string(),
        },
        Scenario {
            name: "intercontinental".to_string(),
            description: "Intercontinental - high BDP + loss".to_string(),
            bandwidth_mbps: 1_000,
            rtt_ms: 150,
            loss_percent: 0.1,
            file_size_bytes: 10 * 1024 * 1024 * 1024,
            file_count: 1,
            purpose: "High BDP + loss".to_string(),
        },
        Scenario {
            name: "degraded-link".to_string(),
            description: "Degraded link - stress test for loss handling".to_string(),
            bandwidth_mbps: 100,
            rtt_ms: 200,
            loss_percent: 2.0,
            file_size_bytes: 1024 * 1024 * 1024, // 1 GB
            file_count: 1,
            purpose: "Stress test for loss handling".to_string(),
        },
        Scenario {
            name: "many-small-files".to_string(),
            description: "Many small files - per-file overhead test".to_string(),
            bandwidth_mbps: 1_000,
            rtt_ms: 50,
            loss_percent: 0.0,
            file_size_bytes: 1024 * 1024, // 1 MB each
            file_count: 10_000,
            purpose: "Per-file overhead test".to_string(),
        },
    ]
}

/// Get a scenario by name.
pub fn get_scenario(name: &str) -> Result<Scenario> {
    all_scenarios()
        .into_iter()
        .find(|s| s.name == name)
        .ok_or_else(|| anyhow::anyhow!("Unknown scenario: {}. Use 'list' to see available scenarios.", name))
}

/// Print all available scenarios.
pub fn list_scenarios() {
    println!("{:<20} {:<15} {:>8} {:>8} {:>6} {:>10} Purpose",
        "Name", "Bandwidth", "RTT", "Loss", "Files", "Size");
    println!("{}", "─".repeat(90));
    for s in all_scenarios() {
        let size = if s.file_count > 1 {
            format!("{} x {}", s.file_count, humansize::format_size(s.file_size_bytes, humansize::BINARY))
        } else {
            humansize::format_size(s.file_size_bytes, humansize::BINARY).to_string()
        };
        println!("{:<20} {:>10} Mbps {:>5} ms {:>5.1}% {:>6} {:>10} {}",
            s.name, s.bandwidth_mbps, s.rtt_ms, s.loss_percent, s.file_count, size, s.purpose);
    }
}
