import { EventEmitter } from 'node:events';
import pg from 'pg';
import { describe, expect, test, vi } from 'vitest';
import { NodePgSession } from '~/node-postgres/session.ts';
import { PgDialect } from '~/pg-core/dialect.ts';
import { sql } from '~/sql/sql.ts';

const { Pool } = pg;

function createMockPool() {
	class MockPoolClient extends EventEmitter {
		query = vi.fn().mockImplementation(async (queryConfig: any) => {
			const text = typeof queryConfig === 'string' ? queryConfig : queryConfig.text;
			if (text.startsWith('begin') || text.startsWith('commit') || text.startsWith('rollback')) {
				return { rows: [] };
			}
			return { rows: [{ id: 1 }] };
		});
		release = vi.fn();
	}

	class MockPool extends EventEmitter {
		connect = vi.fn();
	}

	// Make instanceof Pool work
	Object.setPrototypeOf(MockPool.prototype, Pool.prototype);

	const client = new MockPoolClient();
	const pool = new MockPool();
	pool.connect.mockResolvedValue(client);

	return { pool, client };
}

describe('NodePgSession transaction connection lifecycle', () => {
	test('Test 1 — Suspended callback: outer transaction rejects promptly with original error when connection dies', async () => {
		const { pool, client } = createMockPool();
		const session = new NodePgSession(pool as any, new PgDialect(), {});

		const connectionErr = new Error('Connection terminated unexpectedly');
		let callbackStarted = false;

		const txPromise = session.transaction(async () => {
			callbackStarted = true;
			// Awaiting an external promise that never settles
			await new Promise(() => {});
		});

		// Wait for transaction to begin and callback to run
		while (!callbackStarted) {
			await new Promise((r) => setTimeout(r, 5));
		}

		// Emit connection drop while callback is suspended
		client.emit('error', connectionErr);

		// Outer transaction should reject promptly with original error
		await expect(txPromise).rejects.toThrow('Connection terminated unexpectedly');
	});

	test('Test 2 — Pool client release: client.release(connectionError) is called exactly once', async () => {
		const { pool, client } = createMockPool();
		const session = new NodePgSession(pool as any, new PgDialect(), {});

		const connectionErr = new Error('Connection terminated unexpectedly');
		let callbackStarted = false;

		const txPromise = session.transaction(async () => {
			callbackStarted = true;
			await new Promise(() => {});
		});

		while (!callbackStarted) {
			await new Promise((r) => setTimeout(r, 5));
		}

		client.emit('error', connectionErr);
		await expect(txPromise).rejects.toThrow(connectionErr);

		// Verified: released with connection error so pool destroys it, and exactly once
		expect(client.release).toHaveBeenCalledTimes(1);
		expect(client.release).toHaveBeenCalledWith(connectionErr);
	});

	test('Test 3: queries attempted by a continuing callback fail safely and client cannot be reused', async () => {
		const { pool, client } = createMockPool();
		const session = new NodePgSession(pool as any, new PgDialect(), {});

		const connectionErr = new Error('Connection terminated unexpectedly');
		let resumeCallback: () => void;
		let queryAfterRejectionError: any;

		const txPromise = session.transaction(async (tx) => {
			await new Promise<void>((resolve) => {
				resumeCallback = resolve;
			});
			try {
				await tx.execute(sql`select 1`);
			} catch (e) {
				queryAfterRejectionError = e;
			}
		});

		// Wait for transaction to initialize
		await new Promise((r) => setTimeout(r, 10));

		// Connection drops
		client.emit('error', connectionErr);

		// Outer transaction rejects immediately
		await expect(txPromise).rejects.toThrow('Connection terminated unexpectedly');
		expect(client.release).toHaveBeenCalledWith(connectionErr);

		// Simulate client closed after release with error
		client.query.mockRejectedValueOnce(new Error('Client was closed and is not queryable'));

		// Callback resumes later
		resumeCallback!();
		await new Promise((r) => setTimeout(r, 20));

		// Query safely rejected on closed client (wrapped in DrizzleError with original cause)
		expect(queryAfterRejectionError?.cause?.message ?? queryAfterRejectionError?.message).toBe(
			'Client was closed and is not queryable',
		);

		// Client was never released a second time
		expect(client.release).toHaveBeenCalledTimes(1);
	});

	test('Test 4: normal successful transaction commits and releases client without error', async () => {
		const { pool, client } = createMockPool();
		const session = new NodePgSession(pool as any, new PgDialect(), {});

		const result = await session.transaction(async (tx) => {
			return await tx.execute(sql`select 1`);
		});

		expect(result).toEqual({ rows: [{ id: 1 }] });

		// BEGIN and COMMIT executed
		expect(client.query).toHaveBeenCalledWith(expect.objectContaining({ text: 'begin' }), []);
		expect(client.query).toHaveBeenCalledWith(expect.objectContaining({ text: 'commit' }), []);

		// Released once with undefined error (healthy connection return to pool)
		expect(client.release).toHaveBeenCalledTimes(1);
		expect(client.release).toHaveBeenCalledWith(undefined);
	});

	test('Test 5: callback failure triggers rollback and releases client without duplicate release', async () => {
		const { pool, client } = createMockPool();
		const session = new NodePgSession(pool as any, new PgDialect(), {});

		const callbackErr = new Error('User logic error');

		await expect(session.transaction(async () => {
			throw callbackErr;
		})).rejects.toThrow('User logic error');

		// ROLLBACK was executed
		expect(client.query).toHaveBeenCalledWith(expect.objectContaining({ text: 'rollback' }), []);

		// Released once
		expect(client.release).toHaveBeenCalledTimes(1);
		expect(client.release).toHaveBeenCalledWith(undefined);
	});

	test('Test 6: race condition between connection error and callback rejection', async () => {
		const { pool, client } = createMockPool();
		const session = new NodePgSession(pool as any, new PgDialect(), {});

		const connectionErr = new Error('Connection terminated unexpectedly');
		const callbackErr = new Error('Callback rejected');

		const txPromise = session.transaction(async () => {
			// Trigger both at nearly the same instant
			client.emit('error', connectionErr);
			throw callbackErr;
		});

		// Should reject with either error predictably without crashing or double releasing
		await expect(txPromise).rejects.toThrow();

		// Released exactly once
		expect(client.release).toHaveBeenCalledTimes(1);
	});
});
