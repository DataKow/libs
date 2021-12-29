package org.datakow.core.components;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * The source to this was borrowed from Android java.util.Base64.
 * I just needed to add an additional function, flushBase64()
 * 
 * @author kevin.off
 */
public class Base64OutputStream extends FilterOutputStream {

    private int leftover = 0;
    private int b0, b1, b2;
    private boolean closed = false;

    private final byte[] newline;   // line separator, if needed
    private final int linemax;
    private int linepos = 0;
    
    private final char[] base64 = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
            'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
            'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'
        };

    /**
     * Create an instance
     * 
     * @param os The underlying output stream
     * @param newline Line separator if needed
     * @param linemax Max number of characters before newline or -1
     */
    public Base64OutputStream(OutputStream os, byte[] newline, int linemax) {
        super(os);
        this.newline = newline;
        this.linemax = linemax;
    }

    /**
     * Write a new byte to the output stream
     * 
     * @param b The byte to write
     * @throws IOException If there is an error writing to the output stream
     */
    @Override
    public void write(int b) throws IOException {
        byte[] buf = new byte[1];
        buf[0] = (byte)(b & 0xff);
        write(buf, 0, 1);
    }

    /**
     * Checks if a newline character is needed
     * 
     * @throws IOException If there is an error writing to the output stream
     */
    private void checkNewline() throws IOException {
        if (linemax > 0 && linepos == linemax) {
            out.write(newline);
            linepos = 0;
        }
    }

    /**
     * Writes bytes to the output stream
     * 
     * @param b The bytes to write
     * @param off The offset to start with
     * @param len The length of bytes to write
     * @throws IOException If there is an error writing to the output stream
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (closed)
            throw new IOException("Stream is closed");
        if (off < 0 || len < 0 || off + len > b.length)
            throw new ArrayIndexOutOfBoundsException();
        if (len == 0)
            return;
        if (leftover != 0) {
            if (leftover == 1) {
                b1 = b[off++] & 0xff;
                len--;
                if (len == 0) {
                    leftover++;
                    return;
                }
            }
            b2 = b[off++] & 0xff;
            len--;
            checkNewline();
            out.write(base64[b0 >> 2]);
            out.write(base64[(b0 << 4) & 0x3f | (b1 >> 4)]);
            out.write(base64[(b1 << 2) & 0x3f | (b2 >> 6)]);
            out.write(base64[b2 & 0x3f]);
            linepos += 4;
        }
        int nBits24 = len / 3;
        leftover = len - (nBits24 * 3);
        while (nBits24-- > 0) {
            checkNewline();
            int bits = (b[off++] & 0xff) << 16 |
                       (b[off++] & 0xff) <<  8 |
                       (b[off++] & 0xff);
            out.write(base64[(bits >>> 18) & 0x3f]);
            out.write(base64[(bits >>> 12) & 0x3f]);
            out.write(base64[(bits >>> 6)  & 0x3f]);
            out.write(base64[bits & 0x3f]);
            linepos += 4;
       }
        if (leftover == 1) {
            b0 = b[off++] & 0xff;
        } else if (leftover == 2) {
            b0 = b[off++] & 0xff;
            b1 = b[off++] & 0xff;
        }
    }
    
    /**
     * Flushes the remaining bytes (left over) from the last calculation.
     * Call this before you close the output stream
     * @throws IOException If there is an error writing to the output stream
     */
    public void flushBase64() throws IOException {
        if (!closed) {
            if (leftover == 1) {
                checkNewline();
                out.write(base64[b0 >> 2]);
                out.write(base64[(b0 << 4) & 0x3f]);
                out.write('=');
                out.write('=');
            } else if (leftover == 2) {
                checkNewline();
                out.write(base64[b0 >> 2]);
                out.write(base64[(b0 << 4) & 0x3f | (b1 >> 4)]);
                out.write(base64[(b1 << 2) & 0x3f]);
                out.write('=');
            }
            leftover = 0;
        }
    }
    
    /**
     * Flushes the base64 output stream and 
     * @throws IOException if there is an error closing the stream
     */
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            flushBase64();
            out.close();
        }
    }
}
