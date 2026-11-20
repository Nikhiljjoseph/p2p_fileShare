// Persists partially received files in IndexedDB so a transfer can resume
// after a page reload or a dropped connection.
//
// Two object stores:
//   transfers: one progress record per incoming file
//   parts:     the received data, saved in ~1MB batches keyed by [id, index]
//
// A batch and the record's progress counter are always written in the same
// transaction, so `committedChunks` never claims data that isn't on disk.

const DB_NAME = 'p2p-transfers';
const DB_VERSION = 1;
const TRANSFERS = 'transfers';
const PARTS = 'parts';
const STALE_AFTER_MS = 24 * 60 * 60 * 1000; // abandon partial files after a day

export interface TransferRecord {
    id: string;              // `${clientId}:${fileKey}`
    clientId: string;        // the tab that owns this partial file
    fileKey: string;         // stable identity of the file being received
    fileName: string;
    fileSize: number;
    chunkSize: number;
    totalChunks: number;
    committedChunks: number; // chunks safely stored; resume point
    partCount: number;       // number of batches stored
    updatedAt: number;
}

interface PartRecord {
    id: string;
    index: number;
    blob: Blob;
}

let dbPromise: Promise<IDBDatabase> | null = null;

function openDb(): Promise<IDBDatabase> {
    if (!dbPromise) {
        dbPromise = new Promise((resolve, reject) => {
            const request = indexedDB.open(DB_NAME, DB_VERSION);
            request.onupgradeneeded = () => {
                const db = request.result;
                db.createObjectStore(TRANSFERS, { keyPath: 'id' });
                db.createObjectStore(PARTS, { keyPath: ['id', 'index'] });
            };
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => {
                dbPromise = null;
                reject(request.error);
            };
        });
    }
    return dbPromise;
}

function requestResult<T>(request: IDBRequest<T>): Promise<T> {
    return new Promise((resolve, reject) => {
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
    });
}

function transactionDone(tx: IDBTransaction): Promise<void> {
    return new Promise((resolve, reject) => {
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
        tx.onabort = () => reject(tx.error ?? new DOMException('Transaction aborted', 'AbortError'));
    });
}

// Every part belonging to one transfer, in index order
function partsRange(id: string): IDBKeyRange {
    return IDBKeyRange.bound([id, 0], [id, Infinity]);
}

export function transferRecordId(clientId: string, fileKey: string): string {
    return `${clientId}:${fileKey}`;
}

export const transferStore = {
    async get(id: string): Promise<TransferRecord | undefined> {
        const db = await openDb();
        const store = db.transaction(TRANSFERS).objectStore(TRANSFERS);
        return requestResult(store.get(id) as IDBRequest<TransferRecord | undefined>);
    },

    // Start a transfer from scratch: drop any old data under this id
    async reset(record: TransferRecord): Promise<void> {
        const db = await openDb();
        const tx = db.transaction([TRANSFERS, PARTS], 'readwrite');
        tx.objectStore(PARTS).delete(partsRange(record.id));
        tx.objectStore(TRANSFERS).put(record);
        await transactionDone(tx);
    },

    // Store one batch and advance the progress counter atomically
    async appendPart(record: TransferRecord, index: number, blob: Blob): Promise<void> {
        const db = await openDb();
        const tx = db.transaction([TRANSFERS, PARTS], 'readwrite');
        const part: PartRecord = { id: record.id, index, blob };
        tx.objectStore(PARTS).put(part);
        tx.objectStore(TRANSFERS).put(record);
        await transactionDone(tx);
    },

    // Stitch the stored batches back into one file. Blobs read from IndexedDB
    // are disk-backed, so this doesn't pull the whole file into memory.
    async assemble(id: string): Promise<Blob> {
        const db = await openDb();
        const store = db.transaction(PARTS).objectStore(PARTS);
        const parts = await requestResult(store.getAll(partsRange(id)) as IDBRequest<PartRecord[]>);
        return new Blob(parts.map((part) => part.blob));
    },

    async remove(id: string): Promise<void> {
        const db = await openDb();
        const tx = db.transaction([TRANSFERS, PARTS], 'readwrite');
        tx.objectStore(PARTS).delete(partsRange(id));
        tx.objectStore(TRANSFERS).delete(id);
        await transactionDone(tx);
    },

    // All transfers owned by this tab, finished or not
    async listForClient(clientId: string): Promise<TransferRecord[]> {
        const db = await openDb();
        const store = db.transaction(TRANSFERS).objectStore(TRANSFERS);
        const all = await requestResult(store.getAll() as IDBRequest<TransferRecord[]>);
        return all.filter((record) => record.clientId === clientId);
    },

    // Partial files from tabs that never came back would otherwise live forever
    async removeStale(): Promise<void> {
        const db = await openDb();
        const store = db.transaction(TRANSFERS).objectStore(TRANSFERS);
        const all = await requestResult(store.getAll() as IDBRequest<TransferRecord[]>);
        const cutoff = Date.now() - STALE_AFTER_MS;
        for (const record of all) {
            if (record.updatedAt < cutoff) await this.remove(record.id);
        }
    },
};
