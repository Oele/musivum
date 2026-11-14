// Ported from LiveKit, pkg/sfu/rtxbudget_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the subtests are blocks that carry their names in the
// assertion messages; the budget that may be nil is an `Option`, passed to the functions.

use super::*;

#[test]
fn test_rtx_budget() {
    {
        let name = "no cap without a ratio";
        let b = RtxBudget::new(RtxBudgetLimit {
            burst_bytes: 100,
            ..Default::default()
        });
        assert!(b.is_none(), "{name}");
        assert!(RtxBudget::allow(b.as_ref()), "{name}");
        RtxBudget::spend(b.as_ref(), 1_000_000);
        RtxBudget::on_media_sent(b.as_ref(), 10);
        assert!(RtxBudget::allow(b.as_ref()), "{name}");
    }

    {
        let name = "starts with the burst";
        let b = RtxBudget::new(RtxBudgetLimit {
            max_ratio: 0.3,
            burst_bytes: 3000,
        });
        assert!(RtxBudget::allow(b.as_ref()), "{name}");
        RtxBudget::spend(b.as_ref(), 2999);
        assert!(RtxBudget::allow(b.as_ref()), "{name}");
        RtxBudget::spend(b.as_ref(), 1);
        assert!(!RtxBudget::allow(b.as_ref()), "{name}");
    }

    {
        let name = "burst holds at least one full packet";
        let b = RtxBudget::new(RtxBudgetLimit {
            max_ratio: 0.3,
            ..Default::default()
        });
        RtxBudget::spend(b.as_ref(), (MIN_RTX_BURST_BYTES - 1) as usize);
        assert!(RtxBudget::allow(b.as_ref()), "{name}");
    }

    {
        let name = "an overdraw is paid back from media sent at the ratio";
        let b = RtxBudget::new(RtxBudgetLimit {
            max_ratio: 0.5,
            burst_bytes: 2000,
        });
        RtxBudget::spend(b.as_ref(), 2900); // overdrawn by 900
        assert!(!RtxBudget::allow(b.as_ref()), "{name}");

        RtxBudget::on_media_sent(b.as_ref(), 1800); // pays back exactly 900
        assert!(!RtxBudget::allow(b.as_ref()), "{name}");
        RtxBudget::on_media_sent(b.as_ref(), 2);
        assert!(RtxBudget::allow(b.as_ref()), "{name}");
    }

    {
        let name = "refill stops at the burst";
        let b = RtxBudget::new(RtxBudgetLimit {
            max_ratio: 0.5,
            burst_bytes: 2000,
        });
        RtxBudget::on_media_sent(b.as_ref(), 1_000_000);
        assert!(RtxBudget::allow(b.as_ref()), "{name}"); // refills, but only up to the burst
        RtxBudget::spend(b.as_ref(), 2000);
        assert!(!RtxBudget::allow(b.as_ref()), "{name}");
    }
}

#[test]
fn test_rtx_budget_config_for_kind() {
    let c = RtxBudgetConfig {
        audio: RtxBudgetLimit {
            max_ratio: 0.1,
            ..Default::default()
        },
        video: RtxBudgetLimit {
            max_ratio: 0.3,
            ..Default::default()
        },
    };
    assert_eq!(0.1, c.for_kind(webrtc::RtpCodecType::Audio).max_ratio);
    assert_eq!(0.3, c.for_kind(webrtc::RtpCodecType::Video).max_ratio);
}
