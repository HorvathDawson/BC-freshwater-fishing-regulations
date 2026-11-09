/** (T234) phone-only notifications: the settings kept on the phone, and the background check that raises them. */
export {
  createNotifySettings, dismissRefused, notifyActive, NOTIFY_DEFAULT, noteHeld, putRule, removeRule, setAppVersionNotice, setNotifyOn,
  setRuleOn, setUpdateReady,
  type NotifySettings, type NotifyState,
} from "./settings";
export { runBackgroundCheck, type Notice, type NotifyData } from "./check";
