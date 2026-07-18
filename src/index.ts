import { Request, Response, RequestResponse, Notification } from './studio';

import { get_encoder, get_decoder } from './framing';
import { RpcTransport } from './transport';

import { Mutex } from 'async-mutex';
import { ErrorConditions } from './meta';
import { LightingTarget } from './lighting';
import type {
  Capabilities as LightingCapabilities,
  Notification as LightingNotification,
  Response as LightingResponse,
  ScalarRange as LightingScalarRange,
  Effect as LightingEffect,
  State as LightingState,
  TargetState as LightingTargetState,
} from './lighting';
export { Request, RequestResponse, Response, Notification };
export { LightingTarget };
export type {
  LightingCapabilities,
  LightingEffect,
  LightingNotification,
  LightingResponse,
  LightingScalarRange,
  LightingState,
  LightingTargetState,
};

export interface RpcConnection {
  label: string;
  request_response_readable: ReadableStream<RequestResponse>;
  request_writable: WritableStream<Request>;
  notification_readable: ReadableStream<Notification>;
  current_request: number;
}

export interface CreateRpcConnectionOpts {
  signal?: AbortSignal;
}

export function create_rpc_connection(transport: RpcTransport, opts?: CreateRpcConnectionOpts): RpcConnection {
  let { writable: request_writable, readable: byte_readable } =
    new TransformStream<Request, Uint8Array>({
      transform(chunk, controller) {
        let bytes = Request.encode(chunk).finish();
        controller.enqueue(bytes);
      },
    });

  let reqPipelineClosed = byte_readable
    .pipeThrough(new TransformStream(get_encoder()), { signal: opts?.signal })
    .pipeTo(transport.writable, { signal: opts?.signal });

  reqPipelineClosed.catch((r) => {console.log("Closed error", r); return r}).then(async (reason: any) => {
    await byte_readable.cancel();
    transport.abortController.abort(reason);
  });

  let response_readable = transport.readable
    .pipeThrough(new TransformStream(get_decoder()), { signal: opts?.signal })
    .pipeThrough(
      new TransformStream({
        transform(chunk, controller) {
          controller.enqueue(Response.decode(chunk));
        },
      }),
      { signal: opts?.signal }
    );

  let [a, b] = response_readable.tee();

  let request_response_readable = a.pipeThrough(
    new TransformStream({
      transform(chunk, controller) {
        if (chunk.requestResponse) {
          controller.enqueue(chunk.requestResponse);
        }
      },
    }),
    { signal: opts?.signal }
  );

  let notification_readable = b.pipeThrough(
    new TransformStream({
      transform(chunk, controller) {
        if (chunk.notification) {
          controller.enqueue(chunk.notification);
        }
      },
    }),
    { signal: opts?.signal }
  );

  return {
    label: transport.label,
    request_response_readable,
    request_writable,
    notification_readable,
    current_request: 0,
  };
}

const rpcMutex = new Mutex();

export class NoResponseError extends Error {
  constructor() {
    super("No RPC response received");
    Object.setPrototypeOf(this, NoResponseError.prototype);
  }
}

export class MetaError extends Error {
  readonly condition: ErrorConditions;

  constructor(condition: ErrorConditions) {
    super("Meta error: " + condition);
    this.condition = condition;
    Object.setPrototypeOf(this, MetaError.prototype);
  }
}

export async function call_rpc(
  conn: RpcConnection,
  req: Omit<Request, 'requestId'>
): Promise<RequestResponse> {
  return await rpcMutex.runExclusive(async () => {
    let request: Request = { ...req, requestId: conn.current_request++ };

    let writer = conn.request_writable.getWriter();
    await writer.write(request);
    writer.releaseLock();

    let reader = conn.request_response_readable.getReader();

    let { done, value } = await reader.read();
    reader.releaseLock();

    if (done || !value) {
      throw 'No response';
    }

    if (value.requestId != request.requestId) {
      throw 'Mismatch request IDs';
    }

    if (value.meta?.noResponse) {
      throw new NoResponseError();
    } else if (value.meta?.simpleError !== undefined) {
      throw new MetaError(value.meta.simpleError);
    }

    return value;
  });
}

export class UnexpectedLightingResponseError extends Error {
  readonly operation: string;

  constructor(operation: string) {
    super(`Missing lighting response for ${operation}`);
    this.operation = operation;
    Object.setPrototypeOf(this, UnexpectedLightingResponseError.prototype);
  }
}

function require_lighting_response(
  response: RequestResponse,
  operation: string
): LightingResponse {
  if (!response.lighting) {
    throw new UnexpectedLightingResponseError(operation);
  }

  return response.lighting;
}

function require_lighting_value<T>(
  value: T | undefined,
  operation: string
): T {
  if (value === undefined) {
    throw new UnexpectedLightingResponseError(operation);
  }

  return value;
}

export async function get_lighting_capabilities(
  conn: RpcConnection,
  target: LightingTarget
): Promise<LightingCapabilities> {
  const response = require_lighting_response(
    await call_rpc(conn, {
      lighting: { getCapabilities: { target } },
    }),
    'get capabilities'
  );

  return require_lighting_value(response.getCapabilities, 'get capabilities');
}

export async function try_get_lighting_capabilities(
  conn: RpcConnection,
  target: LightingTarget
): Promise<LightingCapabilities | undefined> {
  try {
    return await get_lighting_capabilities(conn, target);
  } catch (error) {
    if (
      error instanceof MetaError &&
      error.condition === ErrorConditions.RPC_NOT_FOUND
    ) {
      return undefined;
    }

    throw error;
  }
}

export async function get_lighting_state(
  conn: RpcConnection,
  target: LightingTarget
): Promise<LightingTargetState> {
  const response = require_lighting_response(
    await call_rpc(conn, {
      lighting: { getState: { target } },
    }),
    'get state'
  );

  return require_lighting_value(response.getState, 'get state');
}

export async function set_lighting_preview_state(
  conn: RpcConnection,
  target: LightingTarget,
  state: LightingState
): Promise<LightingTargetState> {
  const response = require_lighting_response(
    await call_rpc(conn, {
      lighting: { setPreviewState: { target, state } },
    }),
    'set preview state'
  );

  return require_lighting_value(response.setPreviewState, 'set preview state');
}

export async function check_lighting_unsaved_changes(
  conn: RpcConnection
): Promise<boolean> {
  const response = require_lighting_response(
    await call_rpc(conn, {
      lighting: { checkUnsavedChanges: true },
    }),
    'check unsaved changes'
  );

  return require_lighting_value(
    response.checkUnsavedChanges,
    'check unsaved changes'
  );
}

export async function save_lighting_changes(
  conn: RpcConnection
): Promise<boolean> {
  const response = require_lighting_response(
    await call_rpc(conn, {
      lighting: { saveChanges: true },
    }),
    'save changes'
  );

  return require_lighting_value(response.saveChanges, 'save changes');
}

export async function discard_lighting_changes(
  conn: RpcConnection
): Promise<LightingTargetState> {
  const response = require_lighting_response(
    await call_rpc(conn, {
      lighting: { discardChanges: true },
    }),
    'discard changes'
  );

  return require_lighting_value(response.discardChanges, 'discard changes');
}

export function get_lighting_notification(
  notification: Notification
): LightingNotification | undefined {
  return notification.lighting;
}
