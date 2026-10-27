//---------------------------------------------------------------------------------------------------- TESTS
#[cfg(test)]
mod test {
    use crate::disk::node::Node;
    use crate::disk::pool::Pool;
    use crate::disk::state::State;

    // Line of P2Pool announcing a block of the pool that paid [xmr] at [height].
    fn payout_line(date: &str, xmr: &str, height: u64) -> String {
        format!(
            "NOTICE  {date} P2Pool Your wallet 4AAA got a payout of {xmr} XMR in block {height}"
        )
    }

    #[test]
    fn state_of_the_previous_release() {
        let state = State::to_string(&State::new()).unwrap();
        let previous = state.replace("observers = []\n", "");
        assert_ne!(previous, state);
        // The version check reads the state before any merge.
        assert!(State::from_str(&previous).is_ok());
    }

    #[test]
    fn serde_default_state() {
        let state = State::new();
        let string = State::to_string(&state).unwrap();
        State::from_str(&string).unwrap();
    }
    #[test]
    fn serde_default_node() {
        let node = Node::new_vec();
        let string = Node::to_string(&node).unwrap();
        Node::from_str_to_vec(&string).unwrap();
    }
    #[test]
    fn serde_default_pool() {
        let pool = Pool::new_vec();
        let string = Pool::to_string(&pool).unwrap();
        Pool::from_str_to_vec(&string).unwrap();
    }

    #[test]
    fn serde_custom_node() {
        let node = r#"
			['Local Monero Node']
			ip = "localhost"
			rpc = "18081"
			zmq = "18083"

			['asdf-_. ._123']
			ip = "localhost"
			rpc = "11"
			zmq = "1234"

			['aaa     bbb']
			ip = "192.168.2.333"
			rpc = "1"
			zmq = "65535"
		"#;
        let node = Node::from_str_to_vec(node).unwrap();
        Node::to_string(&node).unwrap();
    }

    #[test]
    fn serde_custom_pool() {
        let pool = r#"
			['Local P2Pool']
			rig = "Gupax_v1.0.0"
			ip = "localhost"
			port = "3333"

			['aaa xx .. -']
			rig = "Gupax"
			ip = "192.168.22.22"
			port = "1"

			['           a']
			rig = "Gupax_v1.0.0"
			ip = "127.0.0.1"
			port = "65535"
		"#;
        let pool = Pool::from_str_to_vec(pool).unwrap();
        Pool::to_string(&pool).unwrap();
    }

    // Make sure we keep the user's old values that are still
    // valid but discard the ones that don't exist anymore.
    #[test]
    fn merge_state() {
        let bad_state = r#"
			[gupax]
			SETTING_THAT_DOESNT_EXIST_ANYMORE = 123123
			simple = false
			auto_update = true
			auto_p2pool = false
			auto_xmrig = false
			ask_before_quit = true
			save_before_quit = true
			p2pool_path = "p2pool/p2pool"
			xmrig_path = "xmrig/xmrig"
			absolute_p2pool_path = ""
			absolute_xmrig_path = ""
			selected_width = 0
			selected_height = 0
			tab = "About"
			ratio = "Width"

			[p2pool]
			SETTING_THAT_DOESNT_EXIST_ANYMORE = "String"
			simple = true
			mini = true
			auto_ping = true
			auto_select = true
			out_peers = 10
			in_peers = 450
			log_level = 6
			arguments = ""
			address = "44hintoFpuo3ugKfcqJvh5BmrsTRpnTasJmetKC4VXCt6QDtbHVuixdTtsm6Ptp7Y8haXnJ6j8Gj2dra8CKy5ewz7Vi9CYW"
			name = "Local Monero Node"
			ip = "localhost"
			rpc = "18081"
			zmq = "18083"
			selected_index = 0
			selected_name = "Local Monero Node"
			selected_ip = "localhost"
			selected_rpc = "18081"
			selected_zmq = "18083"

			[xmrig]
			SETTING_THAT_DOESNT_EXIST_ANYMORE = true
			simple = true
			pause = 0
			simple_rig = ""
			arguments = ""
			tls = false
			keepalive = false
			max_threads = 32
			current_threads = 16
			address = ""
			api_ip = "localhost"
			api_port = "18088"
			name = "Local P2Pool"
			rig = "Gupax_v1.0.0"
			ip = "localhost"
			port = "3333"
			selected_index = 0
			selected_name = "Local P2Pool"
			selected_rig = "Gupax_v1.0.0"
			selected_ip = "localhost"
			selected_port = "3333"

            [xvb]
            token = ""
			[version]
			gupax = "v1.0.0"
			p2pool = "v2.5"
			xmrig = "v6.18.0"
		"#.to_string();
        let merged_state = State::merge(&bad_state).unwrap();
        let merged_state = State::to_string(&merged_state).unwrap();
        println!("{}", merged_state);
        assert!(merged_state.contains("in_peers = 450"));
        assert!(merged_state.contains("log_level = 6"));
        assert!(!merged_state.contains("SETTING_THAT_DOESNT_EXIST_ANYMORE"));
        assert!(merged_state.contains("44hintoFpuo3ugKfcqJvh5BmrsTRpnTasJmetKC4VXCt6QDtbHVuixdTtsm6Ptp7Y8haXnJ6j8Gj2dra8CKy5ewz7Vi9CYW"));
        assert!(merged_state.contains("backup_host = true"));
    }

    #[test]
    fn create_and_serde_gupax_p2pool_api() {
        use crate::disk::gupax_p2pool_api::GupaxP2poolApi;

        // Create the files.
        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        println!("{:#?}", api);

        // Write some fake data.
        let line = "NOTICE  2022-01-27 01:30:23.1377 P2Pool You received a payout of 0.000000000001 XMR in block 2642816";
        api.record_found_block(line).unwrap();
        println!("AFTER WRITE: {:#?}", api);

        // Read
        GupaxP2poolApi::read_all_files_and_update(&mut api).unwrap();
        println!("AFTER READ: {:#?}", api);

        // Assert that the file read mutated the internal struct correctly.
        assert_eq!(api.payout_u64, 1);
        assert_eq!(api.xmr.to_u64(), 1);
        assert!(!api.payout_ord.is_empty());
        assert!(
            api.log
                .contains("2022-01-27 01:30:23.1377 | 0.000000000001 XMR | Block 2,642,816")
        );
    }

    #[test]
    fn ports_of_custom_arguments() {
        use crate::app::submenu_enum::SubmenuP2pool;
        use crate::disk::state::{Node as NodeState, P2pool, StartOptionsMode, XmrigProxy};
        use crate::helper::node::ImgNode;

        // The ZMQ RPC port is another socket than the ZMQ publisher P2Pool uses.
        let node = NodeState {
            simple: false,
            arguments:
                "--zmq-pub tcp://127.0.0.1:18084 --rpc-bind-port 18089 --zmq-rpc-bind-port 18082"
                    .to_string(),
            ..NodeState::default()
        };
        assert_eq!(node.ports(), (18089, 18084));
        let img = ImgNode::new(&node, &StartOptionsMode::Custom);
        assert_eq!((img.rpc_port, img.zmq_port), (18089, 18084));
        let node = NodeState {
            simple: false,
            api_port: String::new(),
            zmq_port: "18084".to_string(),
            ..NodeState::default()
        };
        assert_eq!(node.ports().1, 18084);

        let p2pool = P2pool {
            submenu: SubmenuP2pool::Advanced,
            arguments: "--stratum 0.0.0.0:3334 --mini".to_string(),
            ..P2pool::default()
        };
        assert_eq!(p2pool.stratum_port(), 3334);

        let proxy = XmrigProxy {
            simple: false,
            arguments: "--bind 0.0.0.0:3356 --http-host 127.0.0.1 --http-port 18089".to_string(),
            ..XmrigProxy::default()
        };
        assert_eq!((proxy.bind_port(), proxy.api_port()), (3356, 18089));
    }

    #[test]
    fn local_node_ports_in_p2pool_default_arguments() {
        use crate::disk::state::{P2pool, StartOptionsMode};

        let p2pool = P2pool {
            local_node: true,
            ..P2pool::default()
        };
        let backup_nodes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let path = std::path::Path::new("");
        let args =
            p2pool.start_options(path, &backup_nodes, StartOptionsMode::Simple, 18084, 18089);
        assert!(args.contains("--host 127.0.0.1 --rpc-port 18089 --zmq-port 18084"));
    }

    #[test]
    fn remove_orphaned_payouts() {
        use crate::disk::gupax_p2pool_api::GupaxP2poolApi;

        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        let payout = |xmr: &str, height: u64| payout_line("2026-10-01 10:00:00.0000", xmr, height);
        api.record_found_block(&payout("0.000496318620", 3500000))
            .unwrap();
        api.record_found_block(&payout("0.000000000002", 3500001))
            .unwrap();
        assert_eq!(api.xmr.to_u64(), 496318622);
        // Another Gupax adds a payout.
        api.clone()
            .record_found_block(&payout("0.000000000003", 3500002))
            .unwrap();

        // The first block was orphaned, the other one has no payout.
        let api = std::sync::Mutex::new(api);
        let removed = |heights: &[u64], kept: &[(u64, u64)], orphaned: &[(u64, u64)]| {
            GupaxP2poolApi::remove_payouts(&api, heights, kept, orphaned).unwrap()
        };
        assert_eq!(removed(&[3500000, 3400000], &[], &[]), 1);
        assert_eq!(removed(&[3400000], &[], &[]), 0);
        // The block that replaced an orphaned one paid the user.
        assert_eq!(removed(&[3500001], &[(3500001, 2)], &[]), 0);
        // An orphaned block paid 30 at the height of another block of the pool.
        api.lock()
            .unwrap()
            .record_found_block(&payout("0.000000000030", 3500002))
            .unwrap();
        assert_eq!(removed(&[], &[], &[(3500002, 30)]), 1);

        let mut api = api.into_inner().unwrap();
        api.read_all_files_and_update().unwrap();
        assert_eq!(api.payout_u64, 2);
        assert_eq!(api.xmr.to_u64(), 5);
        assert_eq!(
            api.log,
            "2026-10-01 10:00:00.0000 | 0.000000000002 XMR | Block 3,500,001\n\
             2026-10-01 10:00:00.0000 | 0.000000000003 XMR | Block 3,500,002\n"
        );
    }

    #[test]
    fn merge_synced_payouts() {
        use crate::disk::gupax_p2pool_api::GupaxP2poolApi;
        use crate::xmr::AtomicUnit;

        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        // Payouts seen in the P2Pool output, the first one truncated, 1 atomic unit lower.
        api.record_found_block(&payout_line(
            "2026-09-29 10:00:00.0000",
            "0.000496318619",
            3498000,
        ))
        .unwrap();
        api.record_found_block(&payout_line(
            "2026-10-01 10:00:00.1234",
            "0.000000000002",
            3500001,
        ))
        .unwrap();

        // The synced payouts include them, an older one, and one to another address in a block.
        let payouts = [
            ("2026-09-29 10:00:00.0000", 496318620, 3498000),
            ("2026-10-01 10:00:00.0000", 2, 3500001),
            ("2026-09-30 10:00:00.0000", 5, 3499000),
            ("2026-10-01 10:00:00.0000", 30, 3500001),
        ]
        .map(|(date, amount, height)| (date.to_string(), AtomicUnit::from_u64(amount), height));
        let api = std::sync::Mutex::new(api);
        assert_eq!(GupaxP2poolApi::merge_payouts(&api, &payouts).unwrap(), 2);
        assert_eq!(GupaxP2poolApi::merge_payouts(&api, &payouts).unwrap(), 0);

        let mut api = api.into_inner().unwrap();
        api.read_all_files_and_update().unwrap();
        assert_eq!(api.payout_u64, 4);
        assert_eq!(api.xmr.to_u64(), 496318656);
        assert_eq!(
            api.log,
            "2026-09-29 10:00:00.0000 | 0.000496318619 XMR | Block 3,498,000\n\
             2026-09-30 10:00:00.0000 | 0.000000000005 XMR | Block 3,499,000\n\
             2026-10-01 10:00:00.0000 | 0.000000000030 XMR | Block 3,500,001\n\
             2026-10-01 10:00:00.1234 | 0.000000000002 XMR | Block 3,500,001\n"
        );
    }

    #[test]
    fn record_the_blocks_found_by_p2pool() {
        use crate::disk::gupax_p2pool_api::GupaxP2poolApi;

        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        let payout = |xmr: &str, height: u64| payout_line("2026-10-01 10:00:00.1234", xmr, height);
        api.record_found_block(&payout("0.000000000002", 3500001))
            .unwrap();
        api.record_found_block(&payout("0.000000000005", 3500002))
            .unwrap();
        // Another block of the pool replaced the block 3,500,001.
        api.record_found_block(&payout("0.000000000030", 3500001))
            .unwrap();
        // The block that replaced the block 3,500,002 paid the user nothing.
        let no_payout = "NOTICE  2026-10-01 10:00:00.1234 P2Pool Your wallet 4AAA didn't get a payout in block 3500002";
        api.record_found_block(no_payout).unwrap();
        assert_eq!((api.payout_u64, api.xmr.to_u64()), (1, 30));
        assert_eq!(
            api.log,
            "2026-10-01 10:00:00.1234 | 0.000000000030 XMR | Block 3,500,001\n"
        );
        // The history was deleted.
        std::fs::remove_file(&api.path_log).unwrap();
        api.record_found_block(&payout("0.000000000004", 3500003))
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&api.path_log).unwrap(),
            "2026-10-01 10:00:00.1234 | 0.000000000004 XMR | Block 3,500,003\n"
        );
    }

    #[test]
    #[cfg(unix)]
    fn history_behind_a_link() {
        use crate::disk::gupax_p2pool_api::GupaxP2poolApi;

        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        // The history is a link to a file kept elsewhere.
        let target = dir.path().join("synced_log");
        std::fs::rename(&api.path_log, &target).unwrap();
        std::os::unix::fs::symlink(&target, &api.path_log).unwrap();
        let payout = |xmr: &str, height: u64| payout_line("2026-10-01 10:00:00.0000", xmr, height);
        api.record_found_block(&payout("0.000000000002", 3500001))
            .unwrap();
        assert!(api.path_log.is_symlink());
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "2026-10-01 10:00:00.0000 | 0.000000000002 XMR | Block 3,500,001\n"
        );
        // The file behind the link is unreachable.
        let synced = std::fs::read_to_string(&target).unwrap();
        std::fs::remove_file(&target).unwrap();
        assert!(
            api.record_found_block(&payout("0.000000000003", 3500002))
                .is_err()
        );
        assert!(api.path_log.is_symlink());
        assert_eq!(api.payout_u64, 2);
        // Another block of the pool replaced the block 3,500,001.
        assert!(
            api.record_found_block(&payout("0.000000000030", 3500001))
                .is_err()
        );
        // The payouts are written once the file is back.
        std::fs::write(&target, synced).unwrap();
        api.record_found_block(&payout("0.000000000004", 3500003))
            .unwrap();
        assert_eq!(api.payout_u64, 3);
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "2026-10-01 10:00:00.0000 | 0.000000000003 XMR | Block 3,500,002\n\
             2026-10-01 10:00:00.0000 | 0.000000000030 XMR | Block 3,500,001\n\
             2026-10-01 10:00:00.0000 | 0.000000000004 XMR | Block 3,500,003\n"
        );

        // Gupax starts while the file behind the link is unreachable.
        std::fs::remove_file(&target).unwrap();
        GupaxP2poolApi::create_all_files(dir.path()).unwrap();
        assert!(!target.exists());
        api.read_all_files_and_update().unwrap();
        assert_eq!(api.payout_u64, 0);
    }

    #[test]
    fn history_shared_by_rigs() {
        use crate::disk::gupax_p2pool_api::GupaxP2poolApi;

        let dir = tempfile::tempdir().unwrap();
        let mut rig_a = GupaxP2poolApi::temporary(dir.path());
        let mut rig_b = GupaxP2poolApi::temporary(dir.path());
        // This rig mines to the wallet of the rig A.
        let mut rig_c = GupaxP2poolApi::temporary(dir.path());
        let payout = |xmr: &str, height: u64| payout_line("2026-10-01 10:00:00.1234", xmr, height);
        // The block 3,500,001 paid the wallets of the rigs A and B.
        rig_a
            .record_found_block(&payout("0.000000000002", 3500001))
            .unwrap();
        rig_b
            .record_found_block(&payout("0.000000000030", 3500001))
            .unwrap();
        // The block 3,500,002 paid only the wallet of the rig A.
        rig_a
            .record_found_block(&payout("0.000000000005", 3500002))
            .unwrap();
        rig_c
            .record_found_block(&payout("0.000000000005", 3500002))
            .unwrap();
        let no_payout = "NOTICE  2026-10-01 10:00:00.1234 P2Pool Your wallet 4BBB didn't get a payout in block 3500002";
        rig_b.record_found_block(no_payout).unwrap();
        assert_eq!(
            std::fs::read_to_string(&rig_a.path_log).unwrap(),
            "2026-10-01 10:00:00.1234 | 0.000000000002 XMR | Block 3,500,001\n\
             2026-10-01 10:00:00.1234 | 0.000000000030 XMR | Block 3,500,001\n\
             2026-10-01 10:00:00.1234 | 0.000000000005 XMR | Block 3,500,002\n"
        );
    }

    #[test]
    fn history_written_by_rigs_at_once() {
        use crate::disk::gupax_p2pool_api::GupaxP2poolApi;

        let dir = tempfile::tempdir().unwrap();
        // Each rig mines to its own wallet.
        let rigs = [
            (GupaxP2poolApi::temporary(dir.path()), "0.000000000002"),
            (GupaxP2poolApi::temporary(dir.path()), "0.000000000030"),
        ];
        let path_log = rigs[0].0.path_log.clone();
        let heights = 3500000..3500050;
        // Both P2Pool announce each block at the same time.
        let announced = std::sync::Barrier::new(rigs.len());
        std::thread::scope(|scope| {
            for (mut rig, xmr) in rigs {
                let heights = heights.clone();
                let announced = &announced;
                scope.spawn(move || {
                    for height in heights {
                        let line = payout_line("2026-10-01 10:00:00.1234", xmr, height);
                        announced.wait();
                        rig.record_found_block(&line).unwrap();
                    }
                });
            }
        });
        let log = std::fs::read_to_string(path_log).unwrap();
        assert_eq!(log.lines().count(), 2 * heights.count());
    }

    #[test]
    fn history_edited_elsewhere() {
        use crate::disk::gupax_p2pool_api::GupaxP2poolApi;

        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        // The history was saved by hand without a final line break.
        let edited = "2026-10-01 10:00:00.1234 | 0.000000000002 XMR | Block 3,500,001";
        std::fs::write(&api.path_log, edited).unwrap();
        api.read_all_files_and_update().unwrap();
        let payout = |xmr: &str, height: u64| payout_line("2026-10-01 10:00:00.1234", xmr, height);
        api.record_found_block(&payout("0.000000000007", 3500002))
            .unwrap();
        // The history holds this payout 1 atomic unit lower.
        let truncated = "2026-10-01 10:00:00.1234 | 0.000496318619 XMR | Block 3,500,003\n";
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&api.path_log)
            .unwrap();
        std::io::Write::write_all(&mut file, truncated.as_bytes()).unwrap();
        api.record_found_block(&payout("0.000496318620", 3500003))
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&api.path_log).unwrap(),
            format!(
                "{edited}\n2026-10-01 10:00:00.1234 | 0.000000000007 XMR | Block 3,500,002\n{truncated}"
            )
        );
        assert_eq!((api.payout_u64, api.xmr.to_u64()), (3, 496318628));
    }

    #[test]
    #[cfg(unix)]
    fn history_rewritten_in_place() {
        use crate::disk::gupax_p2pool_api::GupaxP2poolApi;
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        // The history is shared through a hard link, with the rights of its group.
        let shared = dir.path().join("shared_log");
        std::fs::hard_link(&api.path_log, &shared).unwrap();
        let rights = std::fs::Permissions::from_mode(0o660);
        std::fs::set_permissions(&shared, rights).unwrap();
        let payout = |xmr: &str, height: u64| payout_line("2026-10-01 10:00:00.1234", xmr, height);
        api.record_found_block(&payout("0.000000000002", 3500001))
            .unwrap();
        api.record_found_block(&payout("0.000000000005", 3500002))
            .unwrap();
        // Another block of the pool replaced the block 3,500,001.
        api.record_found_block(&payout("0.000000000030", 3500001))
            .unwrap();
        let file = std::fs::metadata(&api.path_log).unwrap();
        assert_eq!(file.ino(), std::fs::metadata(&shared).unwrap().ino());
        assert_eq!(file.mode() & 0o777, 0o660);
        assert_eq!(
            std::fs::read_to_string(&shared).unwrap(),
            "2026-10-01 10:00:00.1234 | 0.000000000005 XMR | Block 3,500,002\n\
             2026-10-01 10:00:00.1234 | 0.000000000030 XMR | Block 3,500,001\n"
        );
    }

    #[test]
    fn history_edited_outside_the_lock() {
        use crate::disk::gupax_p2pool_api::GupaxP2poolApi;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let mut api = GupaxP2poolApi::temporary(dir.path());
        let payout = |xmr: &str, height: u64| payout_line("2026-10-01 10:00:00.1234", xmr, height);
        api.record_found_block(&payout("0.000000000002", 3500001))
            .unwrap();
        let api = Arc::new(Mutex::new(api));
        // Another Gupax is writing the history.
        let file = std::fs::File::open(&api.lock().unwrap().path_log).unwrap();
        file.lock().unwrap();
        // The block 3,500,001 was orphaned.
        let removed = std::thread::spawn({
            let api = Arc::clone(&api);
            move || GupaxP2poolApi::remove_payouts(&api, &[3500001], &[], &[]).unwrap()
        });
        std::thread::sleep(Duration::from_millis(100));
        // The GUI reads the history while the payout is removed.
        assert!(!removed.is_finished());
        assert!(api.try_lock().is_ok());
        // P2Pool announces a payout meanwhile.
        let announced = std::thread::spawn({
            let api = Arc::clone(&api);
            move || {
                let line = payout("0.000000000005", 3500002);
                api.lock().unwrap().record_found_block(&line).unwrap();
            }
        });
        std::thread::sleep(Duration::from_millis(100));
        drop(file);
        assert_eq!(removed.join().unwrap(), 1);
        announced.join().unwrap();
        let api = api.lock().unwrap();
        assert_eq!(
            api.log,
            "2026-10-01 10:00:00.1234 | 0.000000000005 XMR | Block 3,500,002\n"
        );
        assert_eq!(std::fs::read_to_string(&api.path_log).unwrap(), api.log);
    }

    #[test]
    fn convert_hash() {
        use crate::disk::status::Hash;
        let hash = 1.0;
        assert_eq!(Hash::convert(hash, Hash::Hash, Hash::Hash), 1.0);
        assert_eq!(Hash::convert(hash, Hash::Hash, Hash::Kilo), 0.001);
        assert_eq!(Hash::convert(hash, Hash::Hash, Hash::Mega), 0.000_001);
        assert_eq!(Hash::convert(hash, Hash::Hash, Hash::Giga), 0.000_000_001);
        let hash = 1.0;
        assert_eq!(Hash::convert(hash, Hash::Kilo, Hash::Hash), 1_000.0);
        assert_eq!(Hash::convert(hash, Hash::Kilo, Hash::Kilo), 1.0);
        assert_eq!(Hash::convert(hash, Hash::Kilo, Hash::Mega), 0.001);
        assert_eq!(Hash::convert(hash, Hash::Kilo, Hash::Giga), 0.000_001);
        let hash = 1.0;
        assert_eq!(Hash::convert(hash, Hash::Mega, Hash::Hash), 1_000_000.0);
        assert_eq!(Hash::convert(hash, Hash::Mega, Hash::Kilo), 1_000.0);
        assert_eq!(Hash::convert(hash, Hash::Mega, Hash::Mega), 1.0);
        assert_eq!(Hash::convert(hash, Hash::Mega, Hash::Giga), 0.001);
        let hash = 1.0;
        assert_eq!(Hash::convert(hash, Hash::Giga, Hash::Hash), 1_000_000_000.0);
        assert_eq!(Hash::convert(hash, Hash::Giga, Hash::Kilo), 1_000_000.0);
        assert_eq!(Hash::convert(hash, Hash::Giga, Hash::Mega), 1_000.0);
        assert_eq!(Hash::convert(hash, Hash::Giga, Hash::Giga), 1.0);
    }
}
