package testing;

import org.junit.Test;

public class KeyRangeTest {

    public boolean keyInRange(String hashedKey, String lowerRange, String upperRange){
        if (hashedKey.compareTo(upperRange) == 0) {
            return true;
        }
        // if upperRange is larger than lowerRange
        if (upperRange.compareTo(lowerRange) > 0) {
            // hashedkey <= upperRange and hashedkey > lowerRange
            return ((hashedKey.compareTo(upperRange) <= 0) && hashedKey.compareTo(lowerRange) > 0);
        } else {
            // upperRange is smaller than lowerRange (wrap around)
            return ((hashedKey.compareTo(upperRange) <= 0 && (lowerRange.compareTo(hashedKey) > 0))) ||
                    ((hashedKey.compareTo(upperRange) > 0) && (lowerRange.compareTo(hashedKey) < 0));
        }
    }

    @Test
    public void no_wrap_around_hashed_eq_upper(){
        // Case 1: upperRange is larger than lowerRange
        String lowerRange = "1".repeat(32);
        String upperRange = "5".repeat(32);

        // Subcase 1.1: hashedKey == upperRange, is in range
        String hashedKey1 = "5".repeat(32);
        assert(keyInRange(hashedKey1, lowerRange, upperRange));
    }

    @Test
    public void no_wrap_around_hashed_btwn_lower_upper(){
        // Case 1: upperRange is larger than lowerRange
        String lowerRange = "1".repeat(32);
        String upperRange = "5".repeat(32);

        // Subcase 1.2: lowerRange < hashedKey < upperRange, is in range
        String hashedKey2 = "3".repeat(32);
        assert(keyInRange(hashedKey2, lowerRange, upperRange));
    }

    @Test
    public void no_wrap_around_hashed_eq_lower(){
        // Case 1: upperRange is larger than lowerRange
        String lowerRange = "1".repeat(32);
        String upperRange = "5".repeat(32);

        // Subcase 1.3: hashedKey == lowerRange, NOT in range
        String hashedKey3 = "1".repeat(32);
        assert(!keyInRange(hashedKey3, lowerRange, upperRange));
    }

    @Test
    public void no_wrap_around_hashed_less_upper(){
        // Case 1: upperRange is larger than lowerRange
        String lowerRange = "1".repeat(32);
        String upperRange = "5".repeat(32);

        // Subcase 1.4: hashedKey < lowerRange, NOT in range
        String hashedKey4 = "0".repeat(32);
        assert(!keyInRange(hashedKey4, lowerRange, upperRange));
    }

    @Test
    public void no_wrap_around_hashed_greater_lower(){
        // Case 1: upperRange is larger than lowerRange
        String lowerRange = "1".repeat(32);
        String upperRange = "5".repeat(32);

        // Subcase 1.5: hashedKey > upperRange, NOT in range
        String hashedKey5 = "d".repeat(32);
        assert(!keyInRange(hashedKey5, lowerRange, upperRange));
    }


    @Test
    public void wrap_around_hashed_eq_upper(){
        // Case 2: upperRange is smaller than lowerRange (wrap around)
        String lowerRange = "e".repeat(32);
        String upperRange = "3".repeat(32);

        // Subcase 2.1: hashedKey == upperRange, is in range
        String hashedKey1 = "3".repeat(32);
        assert(keyInRange(hashedKey1, lowerRange, upperRange));
    }

    @Test
    public void wrap_around_hashed_less_lower_and_hashed_less_upper(){
        // Case 2: upperRange is smaller than lowerRange (wrap around)
        String lowerRange = "e".repeat(32);
        String upperRange = "3".repeat(32);

        // Subcase 2.2: hashedKey < lowerRange AND hashedKey < upperRange, is in range
        String hashedKey2 = "1".repeat(32);
        assert(keyInRange(hashedKey2, lowerRange, upperRange));
    }

    @Test
    public void wrap_around_hashed_greater_lower_and_hashed_greater_upper(){
        // Case 2: upperRange is smaller than lowerRange (wrap around)
        String lowerRange = "e".repeat(32);
        String upperRange = "3".repeat(32);

        // Subcase 2.3: hashedKey > lowerRange AND hashedKey > upperRange, is in range
        String hashedKey3 = "f".repeat(32);
        assert(keyInRange(hashedKey3, lowerRange, upperRange));
    }

    @Test
    public void wrap_around_hashed_eq_lower(){
        // Case 2: upperRange is smaller than lowerRange (wrap around)
        String lowerRange = "e".repeat(32);
        String upperRange = "3".repeat(32);

        // Subcase 2.4: hashedKey == lowerRange, NOT in range
        String hashedKey4 = "e".repeat(32);
        assert(!keyInRange(hashedKey4, lowerRange, upperRange));
    }

    @Test
    public void wrap_around_hashed_greater_lower_and_hashed_less_upper(){
        // Case 2: upperRange is smaller than lowerRange (wrap around)
        String lowerRange = "e".repeat(32);
        String upperRange = "3".repeat(32);

        // Subcase 2.5: hashedKey > upperRange AND hashedKey < lowerRange, NOT in range
        String hashedKey5 = "a".repeat(32);
        assert(!keyInRange(hashedKey5, lowerRange, upperRange));
    }

    @Test
    public void hash_at_ends_hashed_less_upper(){
        // hashedKey < upperRange (case with hashkey on other side of wrap around), NOT in range
        String lowerRange = "c".repeat(32);
        String upperRange = "f".repeat(32);
        String hashedKey1 = "0".repeat(32);
        assert(!keyInRange(hashedKey1, lowerRange, upperRange));
    }

    @Test
    public void hash_at_ends_hashed_greater_lower(){
        // hashedKey > lowerRange (case with hashkey on other side of wrap around), NOT in range
        String lowerRange = "0".repeat(32);
        String upperRange = "5".repeat(32);
        String hashedKey2 = "f".repeat(32);
        assert(!keyInRange(hashedKey2, lowerRange, upperRange));
    }

}
