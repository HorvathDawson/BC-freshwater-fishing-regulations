/**
 * (T232) The shared declaration for `./envStore`: a development build's run-time choice of data environment, kept on
 * the device and read SYNCHRONOUSLY at start (`config.ts` resolves the endpoints before anything renders). Bundlers
 * pick by platform extension, TypeScript does not.
 */
export declare const envStore: { read(): string | null; write(env: string): void };

/** Start the app again so a new choice is read (the web reloads; a development build reloads its JS). False: it cannot. */
export declare function restart(): boolean;
