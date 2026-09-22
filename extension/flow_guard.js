/* Coordinate only browser requests, never the lifetime of accepted renders. */
class FlowRequestGate {
  constructor({ queueMs = 30000, drainMs = 65000, leaseMs = 120000 } = {}) {
    this.queueMs = queueMs;
    this.drainMs = drainMs;
    this.leaseMs = leaseMs;
    this.active = 0;
    this.submitting = false;
    this.submits = [];
    this.waiters = new Set();
    this.maintenance = null;
  }

  wake() { for (const wake of [...this.waiters]) wake(); }

  waitUntil(ready, deadline, error) {
    if (ready()) return Promise.resolve();
    return new Promise((resolve, reject) => {
      let timer;
      const done = (err) => {
        clearTimeout(timer);
        this.waiters.delete(check);
        err ? reject(new Error(err)) : resolve();
      };
      const check = () => { if (ready()) done(); };
      this.waiters.add(check);
      timer = setTimeout(() => done(error), Math.max(0, deadline - Date.now()));
      check();
    });
  }

  async run(fn, { submit = false, expiresAt = Infinity } = {}) {
    if (this.waiters.size >= 256) throw new Error('FLOW_BUSY_NOT_SUBMITTED');
    const ticket = {};
    if (submit) this.submits.push(ticket);
    let entered = false;
    try {
      const deadline = Math.min(expiresAt, Date.now() + this.queueMs);
      const canEnter = () => !this.maintenance &&
        (!submit || (!this.submitting && this.submits[0] === ticket));
      while (!canEnter()) {
        await this.waitUntil(canEnter, deadline, 'FLOW_BUSY_NOT_SUBMITTED');
      }
      if (Date.now() >= deadline) throw new Error('FLOW_DEADLINE_NOT_SUBMITTED');
      this.active++;
      if (submit) this.submitting = true;
      entered = true;
      return await fn();
    } finally {
      if (entered) {
        this.active--;
        if (submit) this.submitting = false;
      }
      if (submit) this.submits.splice(this.submits.indexOf(ticket), 1);
      this.wake();
    }
  }

  async pause(id) {
    if (!id) throw new Error('MAINTENANCE_ID_REQUIRED');
    if (this.maintenance) {
      if (this.maintenance.id !== id) throw new Error('FLOW_MAINTENANCE_BUSY');
      return this.maintenance.ready;
    }
    const lease = { id, timer: null };
    this.maintenance = lease;
    lease.ready = this.waitUntil(() => this.active === 0,
      Date.now() + this.drainMs, 'FLOW_DRAIN_TIMEOUT').then(() => {
      lease.timer = setTimeout(() => this.release(lease), this.leaseMs);
    }).catch((error) => { this.release(lease); throw error; });
    return lease.ready;
  }

  release(lease) {
    if (this.maintenance !== lease) return;
    clearTimeout(lease.timer);
    this.maintenance = null;
    this.wake();
  }

  async resume(id, beforeRelease = async () => {}) {
    const lease = this.maintenance;
    if (!lease || lease.id !== id) throw new Error('FLOW_MAINTENANCE_EXPIRED');
    if (lease.finishing) throw new Error('FLOW_MAINTENANCE_BUSY');
    lease.finishing = true;
    await lease.ready;
    if (this.maintenance !== lease) throw new Error('FLOW_MAINTENANCE_EXPIRED');
    clearTimeout(lease.timer);
    try { return await beforeRelease(); }
    finally { this.release(lease); }
  }

  snapshot() {
    return { active: this.active, queuedSubmits: this.submits.length,
      submitting: this.submitting, paused: !!this.maintenance };
  }
}

globalThis.FlowRequestGate = FlowRequestGate;
if (typeof module !== 'undefined') module.exports = { FlowRequestGate };
