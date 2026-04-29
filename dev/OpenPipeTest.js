// WinCC Unified Open Pipe — minimal Node.js test program.
//
// Uses net.createConnection which on Windows opens named pipes via the proper
// CreateNamedPipe client API (not as a plain file), giving correct duplex semantics.
//
// Sequence: probe → BrowseTags (paged) → SubscribeTag → watch live notifications
// Each NotifySubscribeTag line shows how many tags changed vs. were suppressed by hasChanged=false.
//
// Requirements:
//   - Node.js 14+
//   - WinCC Unified Runtime running on this host
//   - The user running this command is a member of group "SIMATIC HMI"
//
// Run from the repo root:
//   node dev\OpenPipeTest.js                  (default filter "*", probe first)
//   node dev\OpenPipeTest.js "HMI_Tag_*"      (custom filter)
//   node dev\OpenPipeTest.js skip "HMI_Tag_*" (skip probe, go straight to BrowseTags)

'use strict';

const net = require('net');
const readline = require('readline');

const args = process.argv.slice(2);
let skipProbe = false;
let filter = '*';
if (args[0] && args[0].toLowerCase() === 'skip') {
    skipProbe = true;
    filter = args[1] || '*';
} else if (args[0]) {
    filter = args[0];
}

const PIPE_PATH       = '\\\\.\\pipe\\HmiRuntime';
const BROWSE_COOKIE   = 'openpipe-browse-1';
const SUB_COOKIE      = 'openpipe-sub-1';
const PRE_SEND_DELAY_MS = 1000;
const PROBE_WAIT_MS     = 5000;

log('Pipe path:  ' + PIPE_PATH);
log('Filter:     ' + filter);
log('Skip probe: ' + skipProbe);

let rxCount = 0;
let lastRxTime = Date.now();
let probeAnswered = false;
const browsedTags = [];   // accumulates tag names across pages

const client = net.createConnection(PIPE_PATH);

client.on('connect', () => {
    log('Pipe connected');

    const rl = readline.createInterface({ input: client, crlfDelay: Infinity });
    rl.on('line', (line) => {
        lastRxTime = Date.now();
        rxCount++;
        log('RX #' + rxCount + ': ' + line);
        probeAnswered = true;

        try {
            const msg = JSON.parse(line);
            switch (msg.Message) {

                case 'NotifyBrowseTags': {
                    const tags = (msg.Params && msg.Params.Tags) || [];
                    if (tags.length === 0) {
                        log('BrowseTags complete (empty page) — ' + browsedTags.length + ' tags total');
                        sendSubscribe(browsedTags);
                    } else {
                        tags.forEach(t => browsedTags.push(t.Name));
                        log('  -> page of ' + tags.length + ' tags (total so far: ' + browsedTags.length + ')');
                        sendNext();
                    }
                    break;
                }

                case 'ErrorBrowseTags': {
                    // Server closed the browse session (last partial page already sent).
                    log('ErrorBrowseTags (' + msg.ErrorDescription + ') — treating as browse complete, ' + browsedTags.length + ' tags');
                    sendSubscribe(browsedTags);
                    break;
                }

                case 'NotifySubscribeTag': {
                    const tags = (msg.Params && msg.Params.Tags) || [];
                    const changed   = tags.filter(t => t.hasChanged !== false);
                    const unchanged = tags.filter(t => t.hasChanged === false);
                    log('  -> ' + tags.length + ' total: ' + changed.length + ' changed, ' + unchanged.length + ' unchanged (hasChanged=false)');
                    changed.forEach(t => log('     CHANGED ' + t.Name + ' = ' + t.Value + ' [' + t.Quality + ']'));
                    break;
                }
            }
        } catch (e) {
            log('  (parse error: ' + e.message + ')');
        }
    });

    log('Waiting ' + PRE_SEND_DELAY_MS + 'ms before first send...');
    setTimeout(() => {
        if (skipProbe) {
            sendBrowse();
            return;
        }

        sendLine('PROBE', JSON.stringify({
            Message: 'ReadConfig',
            Params:  ['DefaultPageSize'],
            ClientCookie: 'openpipe-test-probe'
        }));

        const deadline = Date.now() + PROBE_WAIT_MS;
        const waitForProbe = () => {
            if (probeAnswered) {
                log('Probe round-trip OK');
                setTimeout(sendBrowse, 500);
            } else if (Date.now() < deadline) {
                setTimeout(waitForProbe, 100);
            } else {
                log('WARNING: no response to probe after ' + PROBE_WAIT_MS + 'ms');
                log('RT is not reading our bytes. Try restarting WinCC Unified Runtime.');
                setTimeout(sendBrowse, 500);
            }
        };
        setTimeout(waitForProbe, 100);
    }, PRE_SEND_DELAY_MS);
});

client.on('error', (err) => {
    log('Pipe error: ' + err.message);
    if (err.code === 'ENOENT') log('Pipe does not exist — is WinCC Unified Runtime running?');
    else if (err.code === 'EACCES') log('Access denied — is the current user in the SIMATIC HMI group?');
});

client.on('close', () => { log('Pipe closed'); });

function sendBrowse() {
    sendLine('BROWSE', JSON.stringify({
        Message: 'BrowseTags',
        Params:  { Filter: filter },
        ClientCookie: BROWSE_COOKIE
    }));
}

function sendNext() {
    sendLine('NEXT', JSON.stringify({
        Message: 'BrowseTags',
        Params:  'Next',
        ClientCookie: BROWSE_COOKIE
    }));
}

function sendSubscribe(tags) {
    if (tags.length === 0) { log('No tags to subscribe to.'); return; }
    log('Subscribing to ' + tags.length + ' tags...');
    sendLine('SUBSCRIBE', JSON.stringify({
        Message: 'SubscribeTag',
        Params:  { Tags: tags },
        ClientCookie: SUB_COOKIE
    }));
}

function sendLine(label, json) {
    log(label + ' TX: ' + json);
    client.write(json + '\n', 'utf8', () => { log(label + ' TX flushed'); });
}

function log(msg) {
    console.log('[' + new Date().toISOString() + '] ' + msg);
}

// Heartbeat every 5 s
setInterval(() => {
    const sinceRx = Math.round((Date.now() - lastRxTime) / 1000);
    log('heartbeat — RX count=' + rxCount + ', ' + sinceRx + 's since last RX');
}, 5000).unref();
