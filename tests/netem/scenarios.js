// The degraded-network scenarios. Each is DURATION seconds (or its own `duration`) of a call that is already up
// (alice publishes her camera in three simulcast layers, bob watches it in a 1280x720
// element, so the server should send him the highest layer): clean until `at`, impaired
// for `for` seconds with the relay commands of `set`, clean again after. Times are seconds
// from the start of the measured part.
//
// The relay's commands (netem-relay's control protocol): `set <up|down|both>
// [client=<name>] key=value ...`; "up" is client to server, "down" server to client. Without
// `client=` a setting applies to each client's link on its own (alice's and bob's losses are
// independent). Alice only publishes and bob only subscribes, so alice's uplink and bob's
// downlink carry the media; the other two directions carry feedback.
//
// A scenario with `extra: true` is not among `all`: it is run by name. One with `setup` has
// those commands in force from before the two join until bob is subscribed (the connection's
// set-up: ICE, DTLS, SCTP and the first media under impairment), and is clean after.
const DURATION = 60;

const scenarios = {
  baseline: {
    about: 'a clean network',
    set: [],
  },
  'loss-2': {
    about: '2% random loss, both directions, for 20 s',
    at: 15, for: 20,
    set: ['set both loss=0.02'],
  },
  'loss-5-bursts': {
    about: '5% loss in bursts of 8 packets on average, both directions, for 20 s',
    at: 15, for: 20,
    set: ['set both loss=0.05 burst=8'],
  },
  'delay-150-jitter-30': {
    about: '150 ms one-way delay with +-30 ms jitter, both directions, for 20 s',
    at: 15, for: 20,
    set: ['set both delay=150 jitter=30'],
  },
  'cap-down-500': {
    about: 'the subscriber\'s downlink capped to 500 kbit/s (300 ms queue) for 20 s',
    at: 15, for: 20,
    set: ['set down client=bob rate=500 queue=300'],
  },
  'cap-down-500-long': {
    about: 'cap-down-500 with 115 s after the cap instead of 25, for how the frame rate climbs back',
    at: 15, for: 20, duration: 150, extra: true,
    set: ['set down client=bob rate=500 queue=300'],
  },
  'setup-loss-30': {
    about: '30% random loss, both directions, while the two join and bob subscribes; clean after',
    duration: 20, extra: true, setupTimeoutS: 60,
    setup: ['set both loss=0.3'], set: [],
  },
  'cap-up-1000': {
    about: 'the publisher\'s uplink capped to 1000 kbit/s (300 ms queue) for 20 s',
    at: 15, for: 20,
    set: ['set up client=alice rate=1000 queue=300'],
  },
  'blackout-3s': {
    about: 'nothing passes in either direction for 3 s',
    at: 15, for: 3,
    set: ['set both blackout=1'],
  },
};

module.exports = {DURATION, scenarios};
