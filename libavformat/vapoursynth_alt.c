/*
 * VapourSynth support
 * Copyright (c) 2020 Fredrik Melbin
 *
 * This file is part of FFmpeg
 *
 * FFmpeg is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * FFmpeg is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with FFmpeg; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

#include <stdatomic.h>

#include "libavutil/attributes.h"
#include "libavutil/cpu.h"
#include "libavutil/internal.h"
#include "libavutil/time.h"

#include "libavcodec/internal.h"

#include "avformat.h"
#include "demux.h"
#include "internal.h"
#include "config.h"

#include <vapoursynth/VSScript.h>
#include <vapoursynth/VSHelper.h>

typedef struct VapourSynthContext {
    const AVClass *class;
    const VSAPI *vsapi;
    VSScript *script;
    VSNodeRef *node;
    const VSVideoInfo *vi;
    int curr_frame;
    int ncpu;
    _Atomic int async_pending;
} VapourSynthContext;

static void VS_CC async_callback(void *user_data, const VSFrameRef *f, int n,
                                 VSNodeRef *node, const char *error_msg)
{
    AVFormatContext *s = user_data;
    VapourSynthContext *vs = s->priv_data;

    if (!f) {
        av_log(s, AV_LOG_WARNING, "async frame request failed: %s\n",
               error_msg);
    }

    vs->vsapi->freeFrame(f);
    atomic_fetch_sub(&vs->async_pending, 1);
}

static av_cold int map_vsformat(AVFormatContext *s, AVStream *st,
                                const VSFormat *vsformat)
{
    if (vsformat->subSamplingW > 2 || vsformat->subSamplingH > 2)
        return AV_PIX_FMT_NONE;

    switch (vsformat->colorFamily) {
    case cmGray:
        switch (vsformat->bitsPerSample) {
        case 8: return AV_PIX_FMT_GRAY8;
        case 9: return AV_PIX_FMT_GRAY9;
        case 10: return AV_PIX_FMT_GRAY10;
        case 12: return AV_PIX_FMT_GRAY12;
        case 16: return AV_PIX_FMT_GRAY16;
        default: return AV_PIX_FMT_NONE;
        }
    case cmRGB:
        switch (vsformat->bitsPerSample) {
        case 8: return AV_PIX_FMT_GBRP;
        case 9: return AV_PIX_FMT_GBRP9;
        case 10: return AV_PIX_FMT_GBRP10;
        case 12: return AV_PIX_FMT_GBRP12;
        case 14: return AV_PIX_FMT_GBRP14;
        case 16: return AV_PIX_FMT_GBRP16;
        case 32: return AV_PIX_FMT_GBRPF32;
        default: return AV_PIX_FMT_NONE;
        }
    case cmYUV:
    case cmYCoCg:
        switch ((vsformat->subSamplingW << 2) | vsformat->subSamplingH) {
        case ((0 << 2) | 0): /* 4:4:4 */
            switch (vsformat->bitsPerSample) {
            case 8: return AV_PIX_FMT_YUV444P;
            case 9: return AV_PIX_FMT_YUV444P9;
            case 10: return AV_PIX_FMT_YUV444P10;
            case 12: return AV_PIX_FMT_YUV444P12;
            case 14: return AV_PIX_FMT_YUV444P14;
            case 16: return AV_PIX_FMT_YUV444P16;
            default: return AV_PIX_FMT_NONE;
            }
        case ((0 << 2) | 1): /* 4:4:0 */
            switch (vsformat->bitsPerSample) {
            case 8: return AV_PIX_FMT_YUV440P;
            case 10: return AV_PIX_FMT_YUV440P10;
            case 12: return AV_PIX_FMT_YUV440P12;
            default: return AV_PIX_FMT_NONE;
            }
        case ((1 << 2) | 0): /* 4:2:2 */
            switch (vsformat->bitsPerSample) {
            case 8: return AV_PIX_FMT_YUV422P;
            case 9: return AV_PIX_FMT_YUV422P9;
            case 10: return AV_PIX_FMT_YUV422P10;
            case 12: return AV_PIX_FMT_YUV422P12;
            case 14: return AV_PIX_FMT_YUV422P14;
            case 16: return AV_PIX_FMT_YUV422P16;
            default: return AV_PIX_FMT_NONE;
            }
        case ((1 << 2) | 1): /* 4:2:0 */
            switch (vsformat->bitsPerSample) {
            case 8: return AV_PIX_FMT_YUV420P;
            case 9: return AV_PIX_FMT_YUV420P9;
            case 10: return AV_PIX_FMT_YUV420P10;
            case 12: return AV_PIX_FMT_YUV420P12;
            case 14: return AV_PIX_FMT_YUV420P14;
            case 16: return AV_PIX_FMT_YUV420P16;
            default: return AV_PIX_FMT_NONE;
            }
        case ((2 << 2) | 0): /* 4:1:1 */
            return vsformat->bitsPerSample == 8 ? AV_PIX_FMT_YUV411P
                                                : AV_PIX_FMT_NONE;
        case ((2 << 2) | 2): /* 4:1:0 */
            return vsformat->bitsPerSample == 8 ? AV_PIX_FMT_YUV410P
                                                : AV_PIX_FMT_NONE;
        }
    case cmCompat:
        switch (vsformat->id) {
        case pfCompatBGR32: return AV_PIX_FMT_RGB32;
        case pfCompatYUY2: return AV_PIX_FMT_YUYV422;
        default: return AV_PIX_FMT_NONE;
        }
    default:
        return AV_PIX_FMT_NONE;
    }
}

static av_cold int create_video_stream(AVFormatContext *s)
{
    VapourSynthContext *vs = s->priv_data;
    const VSVideoInfo *vi = vs->vi;
    AVStream *st;
    int ret;

    if (!isConstantFormat(vi)) {
        av_log(s, AV_LOG_ERROR,
               "can only open scripts with constant output format\n");
        return AVERROR_INVALIDDATA;
    }

    if (!(st = avformat_new_stream(s, NULL)))
        return AVERROR_UNKNOWN;

    st->id = 0;
    st->codecpar->codec_type = AVMEDIA_TYPE_VIDEO;
    st->codecpar->codec_id   = AV_CODEC_ID_RAWVIDEO;
    st->codecpar->width      = vi->width;
    st->codecpar->height     = vi->height;

    st->avg_frame_rate    = (AVRational) { vi->fpsNum, vi->fpsDen };
    st->start_time        = 0;
    st->duration          = vi->numFrames;
    st->nb_frames         = vi->numFrames;
    avpriv_set_pts_info(st, 32, vi->fpsDen, vi->fpsNum);

    if ((ret = map_vsformat(s, st, vi->format)) == AV_PIX_FMT_NONE) {
        av_log(s, AV_LOG_ERROR, "no matching pixfmt for '%s'\n",
               vi->format->name);
        return AVERROR_INVALIDDATA;
    }
    st->codecpar->format = ret;

    return 0;
}

static av_cold int open_script(AVFormatContext *s)
{
    VapourSynthContext *vs = s->priv_data;
    int ret;

    // Locking is required, because vsscript_evaluateScript changes the
    // process directory internally.
    if (ret = ff_lock_avformat())
        return ret;

    if (vsscript_evaluateFile(&vs->script, s->url, efSetWorkingDir)) {
        ff_unlock_avformat();
        av_log(s, AV_LOG_ERROR, "VapourSynth script evaluation failed\n");
        return AVERROR_EXTERNAL;
    }

    ff_unlock_avformat();

    if (!(vs->node = vsscript_getOutput(vs->script, 0))) {
        av_log(s, AV_LOG_ERROR, "script has no output node\n");
        ret = AVERROR_EXTERNAL;
        goto fail;
    }

    vs->vi = vs->vsapi->getVideoInfo(vs->node);
    if (ret = create_video_stream(s))
        goto fail;

    return 0;
fail:
    vs->vsapi->freeNode(vs->node);
    vsscript_freeScript(vs->script);
    return ret;
}

static av_cold int read_header(AVFormatContext *s)
{
    VapourSynthContext *vs = s->priv_data;
    int ret;

    if (!vsscript_init()) {
        av_log(s, AV_LOG_ERROR,
               "could not initialize VapourSynth environment\n");
        return AVERROR_EXTERNAL;
    }

    if ((ret = vsscript_getApiVersion()) < VSSCRIPT_API_VERSION) {
        av_log(s, AV_LOG_ERROR, "VSScript API too old: %d vs %d\n", ret,
               VSSCRIPT_API_VERSION);
        ret = AVERROR_EXTERNAL;
        goto fail;
    }

    if (!(vs->vsapi = vsscript_getVSApi2(VAPOURSYNTH_API_VERSION))) {
        av_log(s, AV_LOG_ERROR, "VapourSynth API too old: %d required\n",
               VAPOURSYNTH_API_VERSION);
        ret = AVERROR_EXTERNAL;
        goto fail;
    }

    if (ret = open_script(s))
        goto fail;

    vs->ncpu = av_cpu_count();
    return 0;
fail:
    vsscript_finalize();
    return ret;
}

static int read_packet(AVFormatContext *s, AVPacket *pkt)
{
    static const int gbr_order[3] = { 1, 2, 0 };

    AVStream *st = s->streams[0];
    VapourSynthContext *vs = s->priv_data;
    const VSFrameRef *vs_frame = NULL;
    uint8_t *dst_ptr;
    char errbuf[256];
    int i, n, plane, ret;

    if (vs->curr_frame >= vs->vi->numFrames)
        return AVERROR_EOF;

    n = vs->curr_frame++;
    if (st->discard == AVDISCARD_ALL)
        return 0;

    pkt->size = 0;
    for (plane = 0; plane < vs->vi->format->numPlanes; ++plane) {
        int width = vs->vi->width;
        int height = vs->vi->height;

        if (plane == 1 || plane == 2) {
            width >>= vs->vi->format->subSamplingW;
            height >>= vs->vi->format->subSamplingH;
        }

        pkt->size += (int64_t)width * height * vs->vi->format->bytesPerSample;
    }

    if ((ret = av_new_packet(pkt, pkt->size)) < 0)
        return ret;

    pkt->pts = n;
    pkt->dts = n;
    pkt->duration = 1;
    pkt->stream_index = 0;

    vs_frame = vs->vsapi->getFrame(n, vs->node, errbuf, sizeof(errbuf));
    if (!vs_frame) {
        av_log(s, AV_LOG_ERROR, "error retrieving frame %d: %s\n", n, errbuf);
        ret = AVERROR_EXTERNAL;
        goto fail;
    }

    /* Prefetch the subsequent frames. */
    for (i = 0; i < vs->ncpu; ++i) {
        if (i >= vs->vi->numFrames - n)
            break;
        vs->vsapi->getFrameAsync(n + i, vs->node, async_callback, s);
        atomic_fetch_add(&vs->async_pending, 1);
    }

    dst_ptr = pkt->data;
    for (plane = 0; plane < vs->vi->format->numPlanes; ++plane) {
        int src_plane = vs->vi->format->colorFamily == cmRGB ? gbr_order[plane]
                                                             : plane;
        const uint8_t *src_ptr = vs->vsapi->getReadPtr(vs_frame, src_plane);
        int width = vs->vsapi->getFrameWidth(vs_frame, src_plane);
        int height = vs->vsapi->getFrameHeight(vs_frame, src_plane);
        int stride = vs->vsapi->getStride(vs_frame, src_plane);
        int row_size = width * vs->vi->format->bytesPerSample;

        if (vs->vi->format->id == pfCompatBGR32) {
            src_ptr += (int64_t)(height - 1) * stride;
            stride = -stride;
        }

        vs_bitblt(dst_ptr, row_size, src_ptr, stride, row_size, height);
        dst_ptr += (int64_t)row_size * height;
    }

    vs->vsapi->freeFrame(vs_frame);
    return 0;
fail:
    vs->vsapi->freeFrame(vs_frame);
    av_packet_unref(pkt);
    return ret;
}

static av_cold int read_close(AVFormatContext *s)
{
    VapourSynthContext *vs = s->priv_data;

    /* Wait for any async requests to complete. */
    while (atomic_load(&vs->async_pending)) {
        av_usleep(1000);
    }

    vs->vsapi->freeNode(vs->node);
    vsscript_freeScript(vs->script);
    vsscript_finalize();
    return 0;
}

static int read_seek(AVFormatContext *s, int stream_index, int64_t timestamp,
                     int flags)
{
    VapourSynthContext *vs = s->priv_data;

    if (timestamp < 0 || timestamp > INT_MAX || timestamp >= vs->vi->numFrames)
        return AVERROR_EOF;

    vs->curr_frame = timestamp;
    return 0;
}

static const AVClass class_vs = {
    .class_name = "VapourSynth demuxer",
    .item_name  = av_default_item_name,
    .version    = LIBAVUTIL_VERSION_INT,
};

const FFInputFormat ff_vapoursynth_alt_demuxer = {
    .p.name         = "vapoursynth_alt",
    .p.long_name    = NULL_IF_CONFIG_SMALL("VapourSynth demuxer"),
    .p.priv_class   = &class_vs,
    .priv_data_size = sizeof(VapourSynthContext),
    .read_header    = read_header,
    .read_packet    = read_packet,
    .read_close     = read_close,
    .read_seek      = read_seek,
};
