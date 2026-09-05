// k6 load test for the ingestion API's POST /v1/events endpoint.
//
// Run: k6 run loadtest/ingest.js
// Override target/duration: k6 run -e BASE_URL=http://localhost:8080 -e VUS=100 -e DURATION=30s loadtest/ingest.js
//
// The results section of README.md was generated from this exact script
// against the docker-compose stack.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

const errorCounter = new Counter('ingest_errors');

const EVENT_TYPES = ['page_view', 'click', 'purchase', 'lifecycle'];

export const options = {
  scenarios: {
    ramping_load: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 50 },
        { duration: '20s', target: Number(__ENV.VUS) || 200 },
        { duration: __ENV.DURATION || '30s', target: Number(__ENV.VUS) || 200 },
        { duration: '10s', target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<200', 'p(99)<500'],
  },
};

function randomUserID() {
  // Bounded user space (10k users) so the profile builder sees realistic
  // repeat-visit behavior instead of every event being a brand new user.
  return `user-${Math.floor(Math.random() * 10000)}`;
}

export default function () {
  const event = {
    user_id: randomUserID(),
    type: EVENT_TYPES[Math.floor(Math.random() * EVENT_TYPES.length)],
    properties: {
      page: '/products/42',
      source: 'loadtest',
    },
  };

  const res = http.post(`${BASE_URL}/v1/events`, JSON.stringify(event), {
    headers: { 'Content-Type': 'application/json' },
  });

  const ok = check(res, {
    'status is 202': (r) => r.status === 202,
  });
  if (!ok) {
    errorCounter.add(1);
  }

  sleep(0.001);
}
