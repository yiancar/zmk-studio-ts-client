import { ReadableStream, WritableStream } from 'web-streams-polyfill';
import {
  call_rpc,
  MetaError,
  Notification,
  Request,
  RequestResponse,
  RpcConnection,
} from '../src';
import { ErrorConditions } from '../src/meta';

function create_connection(response: RequestResponse): RpcConnection {
  return {
    label: 'test',
    request_response_readable: new ReadableStream<RequestResponse>({
      start(controller) {
        controller.enqueue(response);
        controller.close();
      },
    }),
    request_writable: new WritableStream<Request>(),
    notification_readable: new ReadableStream<Notification>(),
    current_request: 0,
  };
}

describe('call_rpc', () => {
  it('rejects the zero-valued generic error condition', async () => {
    const conn = create_connection({
      requestId: 0,
      meta: { simpleError: ErrorConditions.GENERIC },
    });

    await expect(call_rpc(conn, {})).rejects.toMatchObject<
      Pick<MetaError, 'condition'>
    >({
      condition: ErrorConditions.GENERIC,
    });
  });
});
