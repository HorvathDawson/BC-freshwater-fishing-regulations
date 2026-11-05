/** (T234) phone-only notifications: the settings kept on the phone, and the background check that raises them. */
export {
  createNotifySettings, notifyActive, NOTIFY_DEFAULT, noteHeld, putRule, removeRule, setNotifyOn, setUpdateReady,
  type NotifySettings, type NotifyState,
} from "./settings";
export { runBackgroundCheck, type Notice, type NotifyData } from "./check";
