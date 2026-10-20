/**
 * One write at a time (P01).
 *
 * Every spot write is a read-modify-write of one whole file. Two of them in flight at once (a save from the capture
 * sheet while the weather refresh writes its fill-ins) each read the same list, each add or change one spot, and the
 * second write drops the first one's change. Running every job through one queue makes each read-modify-write atomic
 * against the others. A job that fails rejects its own caller and does not stop the queue.
 */
export function serialised(): <T>(job: () => Promise<T>) => Promise<T> {
  let tail: Promise<unknown> = Promise.resolve();
  return <T>(job: () => Promise<T>): Promise<T> => {
    const run = tail.then(job, job);
    tail = run.catch(() => undefined);
    return run;
  };
}
