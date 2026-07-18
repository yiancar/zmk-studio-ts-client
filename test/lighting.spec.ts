import { ReadableStream, WritableStream } from 'web-streams-polyfill';
import {
  check_lighting_unsaved_changes,
  discard_lighting_changes,
  get_lighting_capabilities,
  get_lighting_notification,
  get_lighting_state,
  LightingCapabilities,
  LightingState,
  LightingTarget,
  LightingTargetState,
  Notification,
  Request,
  RequestResponse,
  RpcConnection,
  save_lighting_changes,
  set_lighting_preview_state,
  try_get_lighting_capabilities,
  UnexpectedLightingResponseError,
} from '../src';
import { ErrorConditions } from '../src/meta';

function create_connection(response: RequestResponse): {
  conn: RpcConnection;
  requests: Request[];
} {
  const requests: Request[] = [];

  const request_response_readable = new ReadableStream<RequestResponse>({
    start(controller) {
      controller.enqueue(response);
      controller.close();
    },
  });
  const request_writable = new WritableStream<Request>({
    write(request) {
      requests.push(request);
    },
  });
  const notification_readable = new ReadableStream<Notification>({
    start(controller) {
      controller.close();
    },
  });

  return {
    conn: {
      label: 'test',
      request_response_readable,
      request_writable,
      notification_readable,
      current_request: 0,
    },
    requests,
  };
}

const target = LightingTarget.LIGHTING_TARGET_UNDERGLOW;
const state: LightingState = {
  on: true,
  hue: 210,
  saturation: 80,
  brightness: 45,
  effect: 2,
  speed: 3,
};
const targetState: LightingTargetState = { target, state };

describe('lighting RPC helpers', () => {
  it('requests target capabilities', async () => {
    const capabilities: LightingCapabilities = {
      target,
      supportsOnOff: true,
      hue: { min: 0, max: 359, step: 1 },
      saturation: { min: 0, max: 100, step: 1 },
      brightness: { min: 0, max: 100, step: 1 },
      speed: { min: 1, max: 5, step: 1 },
      effects: [{ id: 0, displayName: 'Solid' }],
    };
    const { conn, requests } = create_connection({
      requestId: 0,
      lighting: { getCapabilities: capabilities },
    });

    await expect(get_lighting_capabilities(conn, target)).resolves.toEqual(
      capabilities
    );
    expect(requests).toEqual([
      { requestId: 0, lighting: { getCapabilities: { target } } },
    ]);
  });

  it('reports lighting as unsupported on older firmware', async () => {
    const { conn } = create_connection({
      requestId: 0,
      meta: { simpleError: ErrorConditions.RPC_NOT_FOUND },
    });

    await expect(try_get_lighting_capabilities(conn, target)).resolves.toBeUndefined();
  });

  it('gets and previews target state', async () => {
    const getConnection = create_connection({
      requestId: 0,
      lighting: { getState: targetState },
    });

    await expect(get_lighting_state(getConnection.conn, target)).resolves.toEqual(
      targetState
    );
    expect(getConnection.requests[0].lighting?.getState).toEqual({ target });

    const previewConnection = create_connection({
      requestId: 0,
      lighting: { setPreviewState: targetState },
    });

    await expect(
      set_lighting_preview_state(previewConnection.conn, target, state)
    ).resolves.toEqual(targetState);
    expect(previewConnection.requests[0].lighting?.setPreviewState).toEqual(
      targetState
    );
  });

  it('preserves false when checking unsaved changes', async () => {
    const { conn, requests } = create_connection({
      requestId: 0,
      lighting: { checkUnsavedChanges: false },
    });

    await expect(check_lighting_unsaved_changes(conn)).resolves.toBe(false);
    expect(requests[0].lighting?.checkUnsavedChanges).toBe(true);
  });

  it('saves and discards changes', async () => {
    const saveConnection = create_connection({
      requestId: 0,
      lighting: { saveChanges: true },
    });

    await expect(save_lighting_changes(saveConnection.conn)).resolves.toBe(true);
    expect(saveConnection.requests[0].lighting?.saveChanges).toBe(true);

    const discardConnection = create_connection({
      requestId: 0,
      lighting: { discardChanges: targetState },
    });

    await expect(discard_lighting_changes(discardConnection.conn)).resolves.toEqual(
      targetState
    );
    expect(discardConnection.requests[0].lighting?.discardChanges).toBe(true);
  });

  it('rejects a response from the wrong subsystem', async () => {
    const { conn } = create_connection({ requestId: 0 });

    await expect(get_lighting_state(conn, target)).rejects.toBeInstanceOf(
      UnexpectedLightingResponseError
    );
  });

  it('extracts lighting notifications', () => {
    const lighting = { stateChanged: targetState };

    expect(get_lighting_notification({ lighting })).toEqual(lighting);
    expect(get_lighting_notification({})).toBeUndefined();
  });
});
