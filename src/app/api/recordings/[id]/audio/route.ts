import { and, eq } from "drizzle-orm";
import { NextResponse } from "next/server";
import { db } from "@/db";
import { recordings } from "@/db/schema";
import { auth } from "@/lib/auth";
import { createUserStorageProvider } from "@/lib/storage/factory";

const SIGNED_URL_EXPIRY_SECONDS = 3600;

export async function GET(
    request: Request,
    { params }: { params: Promise<{ id: string }> },
) {
    try {
        const session = await auth.api.getSession({
            headers: request.headers,
        });

        if (!session?.user) {
            return NextResponse.json(
                { error: "Unauthorized" },
                { status: 401 },
            );
        }

        const { id } = await params;

        const [recording] = await db
            .select()
            .from(recordings)
            .where(
                and(
                    eq(recordings.id, id),
                    eq(recordings.userId, session.user.id),
                ),
            )
            .limit(1);

        if (!recording) {
            return NextResponse.json(
                { error: "Recording not found" },
                { status: 404 },
            );
        }

        const storage = await createUserStorageProvider(session.user.id);

        // For object stores (S3/B2/R2/MinIO), redirect the browser to a signed
        // URL so range requests hit the bucket directly. This avoids buffering
        // the whole file into the pod's memory on every scrub — which used to
        // OOM/rate-limit for multi-hour recordings.
        const signedUrl = await storage.getSignedUrl(
            recording.storagePath,
            SIGNED_URL_EXPIRY_SECONDS,
        );
        if (/^https?:\/\//.test(signedUrl)) {
            return NextResponse.redirect(signedUrl, 302);
        }

        // Local-storage fallback: stream from disk with Range support.
        const audioBuffer = await storage.downloadFile(recording.storagePath);

        const getContentType = (path: string): string => {
            if (path.endsWith(".mp3")) return "audio/mpeg";
            if (path.endsWith(".opus")) return "audio/opus";
            if (path.endsWith(".wav")) return "audio/wav";
            if (path.endsWith(".m4a")) return "audio/mp4";
            if (path.endsWith(".ogg")) return "audio/ogg";
            if (path.endsWith(".webm")) return "audio/webm";
            return "audio/mpeg";
        };

        const contentType = getContentType(recording.storagePath);
        const fileSize = audioBuffer.length;
        const rangeHeader = request.headers.get("range");

        if (rangeHeader) {
            const rangeMatch = rangeHeader.match(/bytes=(\d+)-(\d*)/);

            if (rangeMatch) {
                const start = parseInt(rangeMatch[1], 10);
                const end = rangeMatch[2]
                    ? parseInt(rangeMatch[2], 10)
                    : fileSize - 1;

                if (
                    start < 0 ||
                    start >= fileSize ||
                    end < 0 ||
                    end >= fileSize ||
                    start > end
                ) {
                    return new NextResponse(null, {
                        status: 416,
                        headers: {
                            "Content-Range": `bytes */${fileSize}`,
                        },
                    });
                }

                const chunkSize = end - start + 1;
                const chunk = audioBuffer.slice(start, end + 1);

                return new NextResponse(new Uint8Array(chunk), {
                    status: 206,
                    headers: {
                        "Content-Type": contentType,
                        "Content-Length": chunkSize.toString(),
                        "Content-Range": `bytes ${start}-${end}/${fileSize}`,
                        "Accept-Ranges": "bytes",
                        "Cache-Control": "public, max-age=31536000, immutable",
                    },
                });
            }
        }

        return new NextResponse(new Uint8Array(audioBuffer), {
            headers: {
                "Content-Type": contentType,
                "Content-Length": fileSize.toString(),
                "Accept-Ranges": "bytes",
                "Cache-Control": "public, max-age=31536000, immutable",
            },
        });
    } catch (error) {
        console.error("Error streaming audio:", error);
        return NextResponse.json(
            { error: "Failed to stream audio" },
            { status: 500 },
        );
    }
}
