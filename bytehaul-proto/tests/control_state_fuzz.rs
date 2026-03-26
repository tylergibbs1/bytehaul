use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use bytehaul_proto::wire::{ControlMessage, ErrorCode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SenderState {
    AwaitingResume,
    AwaitingCompletion,
    Terminal,
    Error,
}

fn drive_sender(state: SenderState, msg: &ControlMessage) -> SenderState {
    match (state, msg) {
        (SenderState::AwaitingResume, ControlMessage::ResumeState { .. }) => {
            SenderState::AwaitingCompletion
        }
        (SenderState::AwaitingResume, ControlMessage::Error { .. }) => SenderState::Error,
        (SenderState::AwaitingCompletion, ControlMessage::ProgressAck { .. }) => {
            SenderState::AwaitingCompletion
        }
        (SenderState::AwaitingCompletion, ControlMessage::TransferComplete { .. }) => {
            SenderState::Terminal
        }
        (SenderState::AwaitingCompletion, ControlMessage::Error { .. }) => SenderState::Error,
        (SenderState::Terminal, _) | (SenderState::Error, _) => state,
        _ => SenderState::Error,
    }
}

fn random_control_message(rng: &mut StdRng) -> ControlMessage {
    match rng.random_range(0..7) {
        0 => ControlMessage::ResumeState {
            blocks_received: vec![0, 1, 2],
        },
        1 => ControlMessage::ProgressAck {
            blocks_confirmed: vec![0, 1],
        },
        2 => ControlMessage::TransferComplete {
            file_blake3: [rng.random(); 32],
        },
        3 => ControlMessage::Error {
            code: ErrorCode::ProtocolError,
            message: "protocol".into(),
        },
        4 => ControlMessage::DeltaRequest { file_index: 0 },
        5 => ControlMessage::DeltaNotAvailable { file_index: 0 },
        _ => ControlMessage::Cancel,
    }
}

#[test]
fn sender_completion_state_machine_fuzz() {
    for seed in 0..128u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut state = SenderState::AwaitingResume;

        for _ in 0..64 {
            let msg = random_control_message(&mut rng);
            state = drive_sender(state, &msg);
            if matches!(state, SenderState::Terminal | SenderState::Error) {
                break;
            }
        }

        assert!(
            matches!(state, SenderState::Terminal | SenderState::Error),
            "seed {seed} never reached a terminal sender state"
        );
    }
}

#[test]
fn valid_sender_sequences_reach_terminal() {
    let seq = [
        ControlMessage::ResumeState {
            blocks_received: vec![],
        },
        ControlMessage::ProgressAck {
            blocks_confirmed: vec![0],
        },
        ControlMessage::TransferComplete {
            file_blake3: [7; 32],
        },
    ];

    let mut state = SenderState::AwaitingResume;
    for msg in &seq {
        state = drive_sender(state, msg);
    }

    assert_eq!(state, SenderState::Terminal);
}

#[test]
fn invalid_sender_sequences_fail_fast() {
    let seq = [
        ControlMessage::DeltaRequest { file_index: 0 },
        ControlMessage::TransferComplete {
            file_blake3: [9; 32],
        },
    ];

    let mut state = SenderState::AwaitingResume;
    for msg in &seq {
        state = drive_sender(state, msg);
        if state == SenderState::Error {
            break;
        }
    }

    assert_eq!(state, SenderState::Error);
}
