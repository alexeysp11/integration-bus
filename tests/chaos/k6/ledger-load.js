// k6 load script for the ledger saga: continuous debit sagas plus top-ups that keep the money supply in equilibrium.
// Scenario description and usage: docs/chaos-engineering/README.md (relative to the repository root).
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { SharedArray } from 'k6/data';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:5201';
const ACCOUNTS_FILE = __ENV.ACCOUNTS_FILE || '/work/accounts.json';
const SUMMARY_FILE = __ENV.SUMMARY_FILE || '/work/k6-summary.json';
const TARGET_RPS = parseInt(__ENV.TARGET_RPS || '200', 10);
const DURATION = __ENV.DURATION || '2m';
const DEBIT_AMOUNT = 10.0;
const TOP_UP_AMOUNT = 60.0;
const TOP_UP_SHARE = 0.15;
const USD = 1;

// Optional bearer token for runs that go through the authenticated gateway
const TOKEN = __ENV.ACCESS_TOKEN || '';

const accounts = new SharedArray('accounts', () => JSON.parse(open(ACCOUNTS_FILE)));

export const acceptedTransactions = new Counter('accepted_transactions');
export const acceptedTopUps = new Counter('accepted_topups');
export const acceptedTopUpAmount = new Counter('accepted_topup_amount');

export const options = {
    scenarios: {
        ledger: {
            executor: 'constant-arrival-rate',
            rate: TARGET_RPS,
            timeUnit: '1s',
            duration: DURATION,
            preAllocatedVUs: Math.max(50, Math.ceil(TARGET_RPS / 2)),
            maxVUs: Math.max(200, TARGET_RPS * 2),
        },
    },
    thresholds: {
        // The API only enqueues work, so it must stay available even while workers are being killed
        http_req_failed: ['rate<0.01'],
        http_req_duration: ['p(95)<500'],
    },
};

function headers() {
    const result = { 'Content-Type': 'application/json' };
    if (TOKEN !== '') {
        result.Authorization = `Bearer ${TOKEN}`;
    }
    return { headers: result };
}

function pickAccount() {
    return accounts[randomIntBetween(0, accounts.length - 1)];
}

export default function () {
    if (Math.random() < TOP_UP_SHARE) {
        const response = http.post(
            `${BASE_URL}/api/v1/accounts/${pickAccount()}/topup`,
            JSON.stringify({ amount: TOP_UP_AMOUNT, currency: USD }),
            headers());

        if (check(response, { 'top-up accepted (202)': (r) => r.status === 202 })) {
            acceptedTopUps.add(1);
            acceptedTopUpAmount.add(TOP_UP_AMOUNT);
        }
        return;
    }

    let source = pickAccount();
    let target = pickAccount();
    while (target === source) {
        target = pickAccount();
    }

    const response = http.post(
        `${BASE_URL}/api/v1/ledger/transaction`,
        JSON.stringify({ sourceAccountId: source, targetAccountId: target, amount: DEBIT_AMOUNT, currency: USD }),
        headers());

    if (check(response, { 'transaction accepted (202)': (r) => r.status === 202 })) {
        acceptedTransactions.add(1);
    }
}

export function handleSummary(data) {
    const counter = (name) => (data.metrics[name] ? data.metrics[name].values.count : 0);
    const summary = {
        acceptedTransactions: counter('accepted_transactions'),
        acceptedTopUps: counter('accepted_topups'),
        acceptedTopUpAmount: counter('accepted_topup_amount'),
        httpRequests: counter('http_reqs'),
        httpFailedRate: data.metrics.http_req_failed ? data.metrics.http_req_failed.values.rate : 0,
        p95Ms: data.metrics.http_req_duration ? data.metrics.http_req_duration.values['p(95)'] : 0,
    };
    return { [SUMMARY_FILE]: JSON.stringify(summary, null, 2), stdout: JSON.stringify(summary, null, 2) + '\n' };
}
