package testing;

import org.junit.Test;


public class KeyRangeTest {

    public boolean keyInRange(String hashedKey, String lowerRange, String upperRange){
        if (hashedKey.compareTo(lowerRange) == 0) {
            return true;
        }
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
    public void no_wrap_around_hashed_eq_lower(){
        // Case 1: lowerRange is larger than upperRange
        String lowerRange = "c".repeat(32);
        String upperRange = "a".repeat(32);

        // Subcase 1.1: hashedKey == lowerRange, is in range
        String hashedKey1 = "c".repeat(32);
        assert(keyInRange(hashedKey1, lowerRange, upperRange));
    }

    @Test
    public void no_wrap_around_hashed_btwn_lower_upper(){
        // Case 1: lowerRange is larger than upperRange
        String lowerRange = "c".repeat(32);
        String upperRange = "a".repeat(32);

        // Subcase 1.2: upperRange < hashedKey < lowerRange, is in range
        String hashedKey2 = "b".repeat(32);
        assert(keyInRange(hashedKey2, lowerRange, upperRange));
    }

    @Test
    public void no_wrap_around_hashed_eq_upper(){
        // Case 1: lowerRange is larger than upperRange
        String lowerRange = "c".repeat(32);
        String upperRange = "a".repeat(32);

        // Subcase 1.3: hashedKey == upperRange, NOT in range
        String hashedKey3 = "a".repeat(32);
        assert(!keyInRange(hashedKey3, lowerRange, upperRange));
    }

    @Test
    public void no_wrap_around_hashed_less_upper(){
        // Case 1: lowerRange is larger than upperRange
        String lowerRange = "c".repeat(32);
        String upperRange = "a".repeat(32);

        // Subcase 1.4: hashedKey < upperRange, NOT in range
        String hashedKey4 = "0".repeat(32);
        assert(!keyInRange(hashedKey4, lowerRange, upperRange));
    }

    @Test
    public void no_wrap_around_hashed_greater_lower(){
        // Case 1: lowerRange is larger than upperRange
        String lowerRange = "c".repeat(32);
        String upperRange = "a".repeat(32);

        // Subcase 1.5: hashedKey > lowerRange, NOT in range
        String hashedKey5 = "f".repeat(32);
        assert(!keyInRange(hashedKey5, lowerRange, upperRange));
    }


    @Test
    public void wrap_around_hashed_eq_lower(){
        // Case 2: lowerRange is smaller than upperRange (wrap around)
        String lowerRange = "b".repeat(32);
        String upperRange = "e".repeat(32);

        // Subcase 2.1: hashedKey == lowerRange, is in range
        String hashedKey1 = "b".repeat(32);
        assert(keyInRange(hashedKey1, lowerRange, upperRange));
    }

    @Test
    public void wrap_around_hashed_less_lower_and_hashed_less_upper(){
        // Case 2: lowerRange is smaller than upperRange (wrap around)
        String lowerRange = "b".repeat(32);
        String upperRange = "e".repeat(32);

        // Subcase 2.2: hashedKey < lowerRange AND hashedKey < upperRange, is in range
        String hashedKey2 = "a".repeat(32);
        assert(keyInRange(hashedKey2, lowerRange, upperRange));
    }

    @Test
    public void wrap_around_hashed_greater_lower_and_hashed_greater_upper(){
        // Case 2: lowerRange is smaller than upperRange (wrap around)
        String lowerRange = "b".repeat(32);
        String upperRange = "e".repeat(32);

        // Subcase 2.3: hashedKey > lowerRange AND hashedKey > upperRange, is in range
        String hashedKey3 = "f".repeat(32);
        assert(keyInRange(hashedKey3, lowerRange, upperRange));
    }

    @Test
    public void wrap_around_hashed_eq_upper(){
        // Case 2: lowerRange is smaller than upperRange (wrap around)
        String lowerRange = "b".repeat(32);
        String upperRange = "e".repeat(32);

        // Subcase 2.4: hashedKey == upperRange, NOT in range
        String hashedKey4 = "e".repeat(32);
        assert(!keyInRange(hashedKey4, lowerRange, upperRange));
    }

    @Test
    public void wrap_around_hashed_greater_lower_and_hashed_less_upper(){
        // Case 2: lowerRange is smaller than upperRange (wrap around)
        String lowerRange = "b".repeat(32);
        String upperRange = "e".repeat(32);

        // Subcase 2.5: hashedKey > lowerRange AND hashedKey < upperRange, NOT in range
        String hashedKey5 = "d".repeat(32);
        assert(!keyInRange(hashedKey5, lowerRange, upperRange));
    }

    @Test
    public void hash_at_ends_hashed_less_upper(){
        // hashedKey < upperRange (case with hashkey on other side of wrap around), NOT in range
        String lowerRange = "c".repeat(32);
        String upperRange = "0".repeat(32);
        String hashedKey1 = "f".repeat(32);
        assert(!keyInRange(hashedKey1, lowerRange, upperRange));
    }

    @Test
    public void hash_at_ends_hashed_greater_lower(){
        // hashedKey > lowerRange (case with hashkey on other side of wrap around), NOT in range
        String lowerRange = "f".repeat(32);
        String upperRange = "d".repeat(32);
        String hashedKey2 = "0".repeat(32);
        assert(!keyInRange(hashedKey2, lowerRange, upperRange));
    }

}
