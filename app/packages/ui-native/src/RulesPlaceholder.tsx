/**
 * WHERE THE REGULATION TABLE GOES. Deliberately empty.
 *
 * What stood here printed a rule's generated `label`, or its `type` with the underscores
 * swapped for spaces, joined by dots — "gear restriction · closure". That says the SHAPE of a
 * rule and never its content, and a reader cannot act on it or check it against the book.
 *
 * It is not being replaced in place, because the table is not a rendering problem. See
 * `pipeline/docs/06-ui-data-contract.md`:
 *
 *   · the client NEVER settles rules into a table. 1,956,787 sections share 2,375 rule sets,
 *     so every answer is precomputed offline (~6,800 of them, about three minutes to build)
 *     and the client looks one up by `set_id` and today's stretch.
 *   · the colour on the left rail is the same lookup, in a second index.
 *
 * Until that index ships, this says so rather than printing something a reader might act on.
 * Swap the body for the real table; nothing else on the screen needs to change.
 */
import { Text, View } from "react-native";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export interface RulesPlaceholderProps {
  palette: Palette;
  /** How many rules the bundle binds here — shown so the screen is not silently empty. */
  count: number;
}

export function RulesPlaceholder({ palette, count }: RulesPlaceholderProps) {
  return (
    <View style={{ gap: 4 }}>
      <Text style={{ ...TYPE.small, color: palette.faint }}>
        {count === 1 ? "1 written rule" : `${count} written rules`} apply here.
      </Text>
      <Text style={{ ...TYPE.small, color: palette.faint }}>
        The table that reads them is being rebuilt.
      </Text>
    </View>
  );
}
