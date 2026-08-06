const SYSTEMD_UNIT: &str = include_str!("../packaging/systemd/ftwdb-shadow.service");
const LAUNCHD_PLIST: &str = include_str!("../packaging/launchd/com.sourceful.ftwdb-shadow.plist");

fn assert_has_line(text: &str, expected: &str) {
    assert!(
        text.lines().any(|line| line.trim() == expected),
        "missing exact setting: {expected}"
    );
}

fn assert_has_fragment(text: &str, expected: &str) {
    assert!(text.contains(expected), "missing setting block: {expected}");
}

fn assert_has_no_network_listener(text: &str) {
    let lower = text.to_ascii_lowercase();
    for forbidden in [
        "0.0.0.0",
        "[::]",
        "tcp://",
        "udp://",
        "listenstream=",
        "listen_datagram",
        "<key>sockets</key>",
        "<key>socktype</key>",
        "<key>inetdcompatibility</key>",
    ] {
        assert!(
            !lower.contains(forbidden),
            "managed service must not expose {forbidden}"
        );
    }
}

#[test]
fn systemd_unit_keeps_the_shadow_endpoint_private_and_stoppable() {
    for setting in [
        "User=ftw",
        "Group=ftw",
        "UMask=0077",
        "RuntimeDirectory=ftwdb-shadow",
        "RuntimeDirectoryMode=0700",
        "StateDirectory=ftwdb-shadow",
        "StateDirectoryMode=0700",
        "ExecStart=/usr/local/libexec/ftwdb-shadow /var/lib/ftwdb-shadow /run/ftwdb-shadow/ftwdb-shadow.sock",
        "Restart=on-failure",
        "KillSignal=SIGTERM",
        "TimeoutStopSec=30s",
        "NoNewPrivileges=yes",
        "ProtectSystem=strict",
        "RestrictAddressFamilies=AF_UNIX",
        "IPAddressDeny=any",
    ] {
        assert_has_line(SYSTEMD_UNIT, setting);
    }

    for forbidden in [
        "User=root",
        "Group=root",
        "DynamicUser=yes",
        "Restart=always",
        "/tmp/",
        "/var/tmp/",
    ] {
        assert!(
            !SYSTEMD_UNIT.contains(forbidden),
            "unsafe systemd setting: {forbidden}"
        );
    }
    assert_has_no_network_listener(SYSTEMD_UNIT);
}

#[test]
fn launchd_job_uses_one_fixed_user_and_unix_socket() {
    for block in [
        "<key>UserName</key>\n    <string>ftw</string>",
        "<key>GroupName</key>\n    <string>ftw</string>",
        "<string>/usr/local/libexec/ftwdb-shadow</string>\n        <string>/var/db/ftwdb-shadow</string>\n        <string>/var/run/ftwdb-shadow/ftwdb-shadow.sock</string>",
        "<key>Umask</key>\n    <integer>63</integer>",
        "<key>RunAtLoad</key>\n    <true/>",
        "<key>SuccessfulExit</key>\n        <false/>",
        "<key>ExitTimeOut</key>\n    <integer>30</integer>",
    ] {
        assert_has_fragment(LAUNCHD_PLIST, block);
    }

    for forbidden in [
        "<string>root</string>",
        "/tmp/",
        "/var/tmp/",
        "NetworkState",
    ] {
        assert!(
            !LAUNCHD_PLIST.contains(forbidden),
            "unsafe launchd setting: {forbidden}"
        );
    }
    assert_has_no_network_listener(LAUNCHD_PLIST);
}
