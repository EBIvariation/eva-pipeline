package uk.ac.ebi.eva.pipeline.parameters.validation;

import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;
import uk.ac.ebi.eva.pipeline.parameters.InputParameters;

import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class InputParametersStudyNameSanitizeTest {

    private InputParameters inputParameters;

    @Before
    public void setUp() {
        inputParameters = new InputParameters();
    }

    private String sanitize(String raw) {
        ReflectionTestUtils.setField(inputParameters, "studyName", raw);
        return inputParameters.getStudyName();
    }

    @Test
    public void allAllowedCharactersArePreservedInStudyName() {
        StringBuilder allowed = new StringBuilder();
        allowed.appendCodePoint(0x09); // Horizontal Tab
        allowed.appendCodePoint(0x0A); // Line Feed (\n)
        allowed.appendCodePoint(0x0D); // Carriage Return (\r)

        // all printable ASCII, including space, digits, letters, punctuation
        IntStream.rangeClosed(0x20, 0x7E).forEach(allowed::appendCodePoint);

        allowed.append("àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ");          // Latin-1 supplement
        allowed.append("αβγδεζηθικλμνξοπρστυφχψω");                 // Greek
        allowed.append("абвгдеёжзийклмнопрстуфхцчшщъыьэюя");        // Cyrillic
        allowed.append("日本語中文한국어");                             // CJK
        allowed.append("مرحبا");                                    // Arabic
        allowed.append("😀🧬🔬");                                  // Emoji

        String input = allowed.toString();

        assertEquals(input, sanitize(input));
    }

    @Test
    public void allStrippedCharactersAreRemovedFromStudyName() {
        StringBuilder input = new StringBuilder();

        input.appendCodePoint(0x0B);    // Vertical Tab
        input.appendCodePoint(0x0C);    // Form Feed
        input.appendCodePoint(0x7F);    // Delete

        // legacy teleprinter/mainframe control codes
        IntStream.rangeClosed(0x00, 0x08).forEach(cp -> {
            input.appendCodePoint(cp);
        });
        IntStream.rangeClosed(0x0E, 0x1F).forEach(cp -> {
            input.appendCodePoint(cp);
        });

        assertEquals("", sanitize(input.toString()));
    }


}