#include <json/json.h>

#include <string>
#include <algorithm>
#include <chrono>

#include <pdal/PointTable.hpp>
#include <pdal/PointView.hpp>
#include <pdal/PipelineManager.hpp>
#include <pdal/Compression.hpp>

#include "pdal-lzma.hpp"

std::chrono::high_resolution_clock::time_point now()
{
    return std::chrono::high_resolution_clock::now();
}

int microsecondsSince(const std::chrono::high_resolution_clock::time_point start)
{
    std::chrono::duration<double> d(now() - start);
    return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
}


class PointTable : public pdal::StreamPointTable
{
public:
    PointTable(pdal::point_count_t capacity) : pdal::StreamPointTable(m_layout),
        m_capacity(capacity)
    {}

    virtual void finalize()
    {
        if (!m_layout.finalized())
        {
            pdal::BasePointTable::finalize();
            m_buf.resize(pointsToBytes(m_capacity + 1));
        }
    }

    pdal::point_count_t capacity() const
        { return m_capacity; }

    std::vector<char> m_buf;

protected:
    virtual char *getPoint(pdal::PointId idx)
        { return m_buf.data() + pointsToBytes(idx); }

private:
    pdal::point_count_t m_capacity;
    pdal::PointLayout m_layout;
};



std::vector<char> lazperf(std::vector<char>& raw, pdal::PointViewPtr view, size_t pointCount)
{

    size_t pointSize = view->layout()->pointSize();
    std::vector<char> output;
    pdal::SignedLazPerfBuf buffer(output);
    pdal::LazPerfCompressor<pdal::SignedLazPerfBuf> compressor(buffer, view->layout()->dimTypes());

    std::vector<char> pt(pointSize);
    for (auto i = 0; i < pointCount; ++i)
    {
        compressor.compress(raw.data() + (i * pointSize), pointSize);
    }
    compressor.done();
    return output;

}


std::vector<int8_t> lzma( const std::vector<char> &input)
{
    LZMA l;

    std::vector<int8_t> compressed;
    std::vector<int8_t> uncompressed;

    uncompressed.reserve(input.size());
    compressed.reserve(input.size());

    l.compress(input, compressed);
//     l.decompress(compressed, uncompressed);

//     bool eq(true);
//     for (size_t i = 0; i < input.size(); ++i)
//     {
//         if (input[i] != uncompressed[i])
//         {
//             eq = false;
//             break;
//         }
//     }
//     std::cout << "check_equal are equal: " << eq << std::endl;
    return compressed;
}



pdal::PointViewSet read(std::string filename,  PointTable& table)
{

    pdal::PipelineManager manager(table);
    manager.makeReader(filename, "");
    manager.execute();
    pdal::PointViewSet views = manager.views();
    return views;

}

int main(int argc, char* argv[])
{
//     std::string filename("/Users/hobu/dev/git/pdal/test/data/local/jacobs.laz");
    std::string filename(argv[1]);
    pdal::PipelineManager manager;
    pdal::Stage& reader = manager.makeReader(filename, "");
    pdal::QuickInfo qi = reader.preview();
    pdal::point_count_t count = qi.m_pointCount;

    auto start = now();
    PointTable table(count);
    pdal::PointViewSet views = read(filename, table);
    pdal::PointViewPtr view = *views.begin();
    auto read_time = microsecondsSince(start);

    Json::Value json;
    Json::Value stats;
    stats["read_time"] = read_time;
    stats["count"] = count;
    stats["size"] = static_cast<Json::UInt>(table.m_buf.size());

    // check laz-perf
    start = now();
    std::vector<char> compressed = lazperf(table.m_buf, view, view->size());
    auto lazperf_time = microsecondsSince(start);
    Json::Value lazperf_stats;
    lazperf_stats["compression_time"] = lazperf_time;
    lazperf_stats["compressed_size"] =  static_cast<Json::UInt>( compressed.size());

    // check lzma
    start = now();
    std::vector<int8_t> lzma_data = lzma(table.m_buf);
    auto lzma_time = microsecondsSince(start);

    Json::Value lzma_stats;
    lzma_stats["compression_time"] = lzma_time;
    lzma_stats["compression_size"] = static_cast<Json::UInt>(lzma_data.size());

    json["lzma"] = lzma_stats;
    json["lazperf"] = lazperf_stats;
    json["stats"] = stats;
    json["Filename"] = filename;

    std::cout << json << std::endl;
    return 0;
}
