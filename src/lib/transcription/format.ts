import type {
    TranscriptionDiarized,
    TranscriptionVerbose,
} from "openai/resources/audio/transcriptions";

type ResponseFormat = "diarized_json" | "json" | "verbose_json";

/**
 * Pick the correct `response_format` for a given transcription model.
 *
 * - Models with "diarize" in the name support speaker-attributed output.
 * - gpt-4o models only accept plain "json" (not "verbose_json").
 * - Everything else (e.g. whisper-1) uses "verbose_json" which includes
 *   detected-language metadata.
 */
export function getResponseFormat(model: string): ResponseFormat {
    if (model.includes("diarize")) return "diarized_json";
    if (model.startsWith("gpt-4o")) return "json";
    return "verbose_json";
}

/**
 * Normalise the transcription response from any supported format into a
 * simple `{ text, detectedLanguage }` pair.
 */
export function parseTranscriptionResponse(
    transcription: unknown,
    responseFormat: ResponseFormat,
): { text: string; detectedLanguage: string | null } {
    if (responseFormat === "diarized_json") {
        const diarized = transcription as TranscriptionDiarized;
        const text = (diarized.segments ?? [])
            .map((seg) => `${seg.speaker}: ${seg.text}`)
            .join("\n");
        return { text, detectedLanguage: null };
    }

    if (responseFormat === "verbose_json") {
        const verbose = transcription as TranscriptionVerbose;
        return {
            text: verbose.text,
            detectedLanguage: verbose.language ?? null,
        };
    }

    // plain "json" — gpt-4o path
    const plain = transcription as { text?: string };
    const text =
        typeof transcription === "string" ? transcription : (plain.text ?? "");
    return { text, detectedLanguage: null };
}
