package testing;

import org.junit.Test;


public class KeyRangeTest {

    public boolean keyRange(String hashedKey, String lowerRange, String upperRange){
        // if lowerRange is larger than upperRange
        if (lowerRange.compareTo(upperRange) > 0) {
            // hashedkey <= lowerRange and hasedkey > upperRange
            return ((hashedKey.compareTo(lowerRange) <= 0) && hashedKey.compareTo(upperRange) > 0);
        } else {
            // lowerRange is smaller than upperRange (wrap around)
            return ((hashedKey.compareTo(lowerRange) <= 0 && (upperRange.compareTo(hashedKey) > 0))) ||
                    ((hashedKey.compareTo(lowerRange) > 0) && (upperRange.compareTo(hashedKey) < 0));
        }
    }

    @Test
    public void no_wrap_around(){
        // Case 1: lowerRange is larger than upperRange
        String lowerRange = "c".repeat(32);
        String upperRange = "a".repeat(32);

        // Subcase 1.1: hashedKey == lowerRange, is in range
        String hashedKey1 = "c".repeat(32);
        assert(keyRange(hashedKey1, lowerRange, upperRange));

        // Subcase 1.2: upperRange < hashedKey < lowerRange, is in range
        String hashedKey2 = "b".repeat(32);
        assert(keyRange(hashedKey2, lowerRange, upperRange));

        // Subcase 1.3: hashedKey == upperRange, NOT in range
        String hashedKey3 = "a".repeat(32);
        assert(!keyRange(hashedKey3, lowerRange, upperRange));

        // Subcase 1.4: hashedKey < upperRange, NOT in range
        String hashedKey4 = "0".repeat(32);
        assert(!keyRange(hashedKey4, lowerRange, upperRange));

        // Subcase 1.5: hashedKey > lowerRange, NOT in range
        String hashedKey5 = "f".repeat(32);
        assert(!keyRange(hashedKey5, lowerRange, upperRange));
    }

    @Test
    public void wrap_around(){
        // Case 2: lowerRange is smaller than upperRange (wrap around)
        String lowerRange = "b".repeat(32);
        String upperRange = "e".repeat(32);

        // Subcase 2.1: hashedKey == lowerRange, is in range
        String hashedKey1 = "b".repeat(32);
        assert(keyRange(hashedKey1, lowerRange, upperRange));

        // Subcase 2.2: hashedKey < lowerRange AND hashedKey < upperRange, is in range
        String hashedKey2 = "a".repeat(32);
        assert(keyRange(hashedKey2, lowerRange, upperRange));

        // Subcase 2.3: hashedKey > lowerRange AND hashedKey > upperRange, is in range
        String hashedKey3 = "f".repeat(32);
        assert(keyRange(hashedKey3, lowerRange, upperRange));

        // Subcase 2.4: hashedKey == upperRange, NOT in range
        String hashedKey4 = "e".repeat(32);
        assert(!keyRange(hashedKey4, lowerRange, upperRange));

        // Subcase 2.5: hashedKey > lowerRange AND hashedKey < upperRange, NOT in range
        String hashedKey5 = "d".repeat(32);
        assert(!keyRange(hashedKey5, lowerRange, upperRange));
    }

    @Test
    public void hash_at_ends(){
        String lowerRange;
        String upperRange;

        // Subcase 1.4: hashedKey < upperRange (case with hashkey on other side of wrap around), NOT in range
        lowerRange = "c".repeat(32);
        upperRange = "0".repeat(32);
        String hashedKey1 = "f".repeat(32);
        assert(!keyRange(hashedKey1, lowerRange, upperRange));

        // Subcase 1.5: hashedKey > lowerRange (case with hashkey on other side of wrap around), NOT in range
        lowerRange = "f".repeat(32);
        upperRange = "d".repeat(32);
        String hashedKey2 = "0".repeat(32);
        assert(!keyRange(hashedKey2, lowerRange, upperRange));
    }

}
