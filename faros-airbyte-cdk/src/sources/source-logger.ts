import {AirbyteLogger, shouldWriteLog} from '../logger';
import {
  AirbyteLogLevel,
  AirbyteLogLevelOrder,
  AirbyteMessage,
  AirbyteSourceLog,
  AirbyteSourceLogsMessage,
  AirbyteState,
  isAirbyteLog,
} from '../protocol';

const MAX_BATCH_SIZE_KB = 100 * 1024;

export class AirbyteSourceLogger extends AirbyteLogger {
  readonly batch: AirbyteSourceLog[] = [];
  private totalSize = 0;

  private _getState: () => AirbyteState;

  constructor(level?: AirbyteLogLevel) {
    super(level);
  }

  set getState(getState: () => AirbyteState) {
    this._getState = getState;
  }

  override write(msg: AirbyteMessage): boolean {
    const ok = super.write(msg);

    if (isAirbyteLog(msg) && shouldWriteLog(msg, this.level)) {
      const sourceLog: AirbyteSourceLog = {
        timestamp: Date.now(),
        message: {
          level: AirbyteLogLevelOrder(msg.log.level),
          msg: msg.log.message,
          stackTrace: msg.log.stack_trace,
        },
      };

      this.batch.push(sourceLog);
      this.totalSize += JSON.stringify(sourceLog).length;
      if (this.totalSize > MAX_BATCH_SIZE_KB && this._getState) {
        return this.flush() && ok;
      }
    }

    return ok;
  }

  flush(): boolean {
    if (!this.batch.length || !this._getState) {
      return true;
    }
    const ok = super.write(
      new AirbyteSourceLogsMessage({data: this._getState()}, this.batch)
    );
    this.batch.length = 0; // clears the array
    this.totalSize = 0;
    return ok;
  }
}
