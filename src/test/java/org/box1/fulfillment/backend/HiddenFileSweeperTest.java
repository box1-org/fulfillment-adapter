package org.box1.fulfillment.backend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class HiddenFileSweeperTest {
    @Test
    void onlyNonTmpDotNamesAreSwept() {
        assertTrue(HiddenFileSweeper.isHiddenUnexpected(".notes"));
        assertTrue(HiddenFileSweeper.isHiddenUnexpected(".RES.ful_1.000001.rsl_x.erp1"));
        assertFalse(HiddenFileSweeper.isHiddenUnexpected(".RES.ful_1.000001.rsl_x.erp1.tmp"));
        assertFalse(HiddenFileSweeper.isHiddenUnexpected("."));
        assertFalse(HiddenFileSweeper.isHiddenUnexpected(".."));
        assertFalse(HiddenFileSweeper.isHiddenUnexpected("notes.txt"));
        assertEquals("_notes_x", HiddenFileSweeper.safeName(".notes/x"));
    }
}
