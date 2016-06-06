#pragma once

#include <lzma.h>
#include <vector>


class LZMA
{

public:
    LZMA(uint32_t mode=LZMA_PRESET_EXTREME)
    {
        compstrm = LZMA_STREAM_INIT;
        decompstrm = LZMA_STREAM_INIT;
        initCompressor(mode);
        initDecompressor();
    }

    void compress(const std::vector<char>& input, std::vector<int8_t>& output)
    {

        // init compressor
        std::vector<int8_t> internal(output);
        internal.reserve(input.size());

        compstrm.next_in = (unsigned char*)input.data();
        compstrm.avail_in = input.size();
        compstrm.next_out = (unsigned char*) internal.data();
        compstrm.avail_out = internal.capacity();

        lzma_ret ret = lzma_code(&compstrm, LZMA_FINISH);
        lzma_end(&compstrm);

        for (size_t i = 0; i < compstrm.total_out; ++i)
        {
            output.push_back(internal[i]);
        }
        if (ret == LZMA_STREAM_END)
        {
            return;
        }

        //
        // It's not LZMA_OK nor LZMA_STREAM_END,
        // so it must be an error code. See lzma/base.h
        // (src/liblzma/api/lzma/base.h in the source package
        // or e.g. /usr/include/lzma/base.h depending on the
        // install prefix) for the list and documentation of
        // possible values. Most values listen in lzma_ret
        // enumeration aren't possible in this example.
        const char *msg;
        switch (ret) {
        case LZMA_MEM_ERROR:
            msg = "Memory allocation failed";
            break;

        case LZMA_DATA_ERROR:
            // This error is returned if the compressed
            // or uncompressed size get near 8 EiB
            // (2^63 bytes) because that's where the .xz
            // file format size limits currently are.
            // That is, the possibility of this error
            // is mostly theoretical unless you are doing
            // something very unusual.
            //
            // Note that compstrm->total_in and compstrm->total_out
            // have nothing to do with this error. Changing
            // those variables won't increase or decrease
            // the chance of getting this error.
            msg = "File size limits exceeded";
            break;

        default:
            // This is most likely LZMA_PROG_ERROR, but
            // if this program is buggy (or liblzma has
            // a bug), it may be e.g. LZMA_BUF_ERROR or
            // LZMA_OPTIONS_ERROR too.
            //
            // It is inconvenient to have a separate
            // error message for errors that should be
            // impossible to occur, but knowing the error
            // code is important for debugging. That's why
            // it is good to print the error code at least
            // when there is no good error message to show.
            msg = "Unknown error, possibly a bug";
            break;
        }
        throw std::runtime_error("compress error" + std::string(msg));
    }
    void decompress(const std::vector<int8_t>& input, std::vector<int8_t>& output)
    {
        std::vector<int8_t> internal(output);
        internal.reserve(output.capacity());

        decompstrm.next_in = (unsigned char*)input.data();
        decompstrm.avail_in = input.size();
        decompstrm.total_in = 0;
        decompstrm.next_out = (unsigned char*) internal.data();
        decompstrm.avail_out = internal.capacity();

        lzma_ret ret = lzma_code(&decompstrm, LZMA_FINISH);
        lzma_end(&decompstrm);

        if (ret == LZMA_STREAM_END)
        {
            for (size_t i = 0; i < decompstrm.total_out; ++i)
            {
                output.push_back(internal[i]);
            }
            output.resize(decompstrm.total_out);
            return;
        }

        //
        // It's not LZMA_OK nor LZMA_STREAM_END,
        // so it must be an error code. See lzma/base.h
        // (src/liblzma/api/lzma/base.h in the source package
        // or e.g. /usr/include/lzma/base.h depending on the
        // install prefix) for the list and documentation of
        // possible values. Most values listen in lzma_ret
        // enumeration aren't possible in this example.
        const char *msg;
        switch (ret) {
        case LZMA_MEM_ERROR:
            msg = "Memory allocation failed";
            break;

        case LZMA_DATA_ERROR:
            // This error is returned if the compressed
            // or uncompressed size get near 8 EiB
            // (2^63 bytes) because that's where the .xz
            // file format size limits currently are.
            // That is, the possibility of this error
            // is mostly theoretical unless you are doing
            // something very unusual.
            //
            // Note that compstrm->total_in and compstrm->total_out
            // have nothing to do with this error. Changing
            // those variables won't increase or decrease
            // the chance of getting this error.
            msg = "File size limits exceeded";
            break;

        default:
            // This is most likely LZMA_PROG_ERROR, but
            // if this program is buggy (or liblzma has
            // a bug), it may be e.g. LZMA_BUF_ERROR or
            // LZMA_OPTIONS_ERROR too.
            //
            // It is inconvenient to have a separate
            // error message for errors that should be
            // impossible to occur, but knowing the error
            // code is important for debugging. That's why
            // it is good to print the error code at least
            // when there is no good error message to show.
            msg = "Unknown error, possibly a bug";
            break;
        }
        throw std::runtime_error("compress error" + std::string(msg));
    }


    void initCompressor(uint32_t LZMA_MODE)
    {
        // Initialize the encoder using a preset. Set the integrity to check
        // to CRC64, which is the default in the xz command line tool. If
        // the .xz file needs to be decompressed with XZ Embedded, use
        // LZMA_CHECK_CRC32 instead.
        lzma_ret ret = lzma_easy_encoder(&compstrm, LZMA_MODE, LZMA_CHECK_CRC64);

        // Return successfully if the initialization went fine.
        if (ret == LZMA_OK)
            return ;

        // Something went wrong. The possible errors are documented in
        // lzma/container.h (src/liblzma/api/lzma/container.h in the source
        // package or e.g. /usr/include/lzma/container.h depending on the
        // install prefix).
        const char *msg;
        switch (ret) {
        case LZMA_MEM_ERROR:
            msg = "Memory allocation failed";
            break;

        case LZMA_OPTIONS_ERROR:
            msg = "Specified preset is not supported";
            break;

        case LZMA_UNSUPPORTED_CHECK:
            msg = "Specified integrity check is not supported";
            break;

        default:
            // This is most likely LZMA_PROG_ERROR indicating a bug in
            // this program or in liblzma. It is inconvenient to have a
            // separate error message for errors that should be impossible
            // to occur, but knowing the error code is important for
            // debugging. That's why it is good to print the error code
            // at least when there is no good error message to show.
            msg = "Unknown error, possibly a bug";
            break;
        }
        throw std::runtime_error("LZMA init failed: " + std::string(msg));
    }

    void initDecompressor()
    {
        // Initialize a .xz decoder. The decoder supports a memory usage limit
        // and a set of flags.
        //
        // The memory usage of the decompressor depends on the settings used
        // to compress a .xz file. It can vary from less than a megabyte to
        // a few gigabytes, but in practice (at least for now) it rarely
        // exceeds 65 MiB because that's how much memory is required to
        // decompress files created with "xz -9". Settings requiring more
        // memory take extra effort to use and don't (at least for now)
        // provide significantly better compression in most cases.
        //
        // Memory usage limit is useful if it is important that the
        // decompressor won't consume gigabytes of memory. The need
        // for limiting depends on the application. In this example,
        // no memory usage limiting is used. This is done by setting
        // the limit to UINT64_MAX.
        //
        // The .xz format allows concatenating compressed files as is:
        //
        //     echo foo | xz > foobar.xz
        //     echo bar | xz >> foobar.xz
        //
        // When decompressing normal standalone .xz files, LZMA_CONCATENATED
        // should always be used to support decompression of concatenated
        // .xz files. If LZMA_CONCATENATED isn't used, the decoder will stop
        // after the first .xz stream. This can be useful when .xz data has
        // been embedded inside another file format.
        //
        // Flags other than LZMA_CONCATENATED are supported too, and can
        // be combined with bitwise-or. See lzma/container.h
        // (src/liblzma/api/lzma/container.h in the source package or e.g.
        // /usr/include/lzma/container.h depending on the install prefix)
        // for details.
        lzma_ret ret = lzma_auto_decoder(
                &decompstrm, UINT64_MAX, LZMA_TELL_UNSUPPORTED_CHECK);

        // Return successfully if the initialization went fine.
        if (ret == LZMA_OK)
            return ;

        // Something went wrong. The possible errors are documented in
        // lzma/container.h (src/liblzma/api/lzma/container.h in the source
        // package or e.g. /usr/include/lzma/container.h depending on the
        // install prefix).
        //
        // Note that LZMA_MEMLIMIT_ERROR is never possible here. If you
        // specify a very tiny limit, the error will be delayed until
        // the first headers have been parsed by a call to lzma_code().
        const char *msg;
        switch (ret) {
        case LZMA_MEM_ERROR:
            msg = "Memory allocation failed";
            break;

        case LZMA_OPTIONS_ERROR:
            msg = "Unsupported decompressor flags";
            break;

        default:
            // This is most likely LZMA_PROG_ERROR indicating a bug in
            // this program or in liblzma. It is inconvenient to have a
            // separate error message for errors that should be impossible
            // to occur, but knowing the error code is important for
            // debugging. That's why it is good to print the error code
            // at least when there is no good error message to show.
            msg = "Unknown error, possibly a bug";
            break;
        }


        throw std::runtime_error("LZMA init failed: " + std::string(msg));
    }

    lzma_stream compstrm;
    lzma_stream decompstrm;

};


