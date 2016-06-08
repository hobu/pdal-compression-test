#include <json/json.h>

#include <string>
#include <algorithm>
#include <chrono>

#include <pdal/PointTable.hpp>
#include <pdal/PointView.hpp>
#include <pdal/LasWriter.hpp>
#include <pdal/BufferReader.hpp>
#include <pdal/PipelineManager.hpp>
#include <pdal/Compression.hpp>

#include <sys/stat.h>
#include <cstdio>

#include "pdal-lzma.hpp"

size_t getFileSize(const std::string& filename)
{
    struct stat st;
    stat(filename.c_str(), &st);
    return st.st_size;
}

std::chrono::high_resolution_clock::time_point now()
{
    return std::chrono::high_resolution_clock::now();
}

int microsecondsSince(const std::chrono::high_resolution_clock::time_point start)
{
    std::chrono::duration<double> d(now() - start);
    return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
}


std::vector<std::string> splitpath(
  const std::string& str
  , const std::set<char> delimiters)
{
	// http://stackoverflow.com/questions/8520560/get-a-file-name-from-a-path
	std::vector<std::string> result;

	char const* pch = str.c_str();
	char const* start = pch;
	for(; *pch; ++pch)
	{
		if (delimiters.find(*pch) != delimiters.end())
		{
			if (start != pch)
			{
				std::string str(start, pch);
				result.push_back(str);
			}
			else
			{
				result.push_back("");
			}
			start = pch + 1;
		}
	}
	result.push_back(start);

	return result;
}


class PointTable : public pdal::StreamPointTable
{
public:
    PointTable(pdal::point_count_t capacity)
		: pdal::StreamPointTable(m_layout)
		, m_count(0)
        , m_capacity(capacity)
    {}

    virtual void finalize()
    {
        if (!m_layout.finalized())
        {
            pdal::BasePointTable::finalize();
            m_buf.resize(pointsToBytes(m_capacity + 1));
        }
    }
    virtual pdal::PointId addPoint()
	{
		if (m_count == m_capacity)
			return m_capacity;
		else
			return m_count++;
	}
    pdal::point_count_t capacity() const
        { return m_capacity; }

    std::vector<char> m_buf;
	pdal::point_count_t m_count;

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
	pdal::DimTypeList types = view->layout()->dimTypes();
    pdal::LazPerfCompressor<pdal::SignedLazPerfBuf> compressor(buffer, types);

	char* position = raw.data();
    for (auto i = 0; i < pointCount; ++i)
    {
        compressor.compress(position, pointSize);
		position = position + pointSize;
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
//
//     bool eq(true);
//     bool empty(true);
//     for (size_t i = 0; i < input.size(); ++i)
//     {
//		if (input[0] != 0) empty = false;
//         if (input[i] != uncompressed[i])
//         {
//             eq = false;
//             break;
//         }
//     }
//     std::cout << "check_equal are equal: " << eq << std::endl;
//     std::cout << "empty: " << empty << std::endl;
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

std::string writeLaz(const std::string& filename, pdal::PointViewPtr view)
{
	std::string tmp;
	pdal::Utils::getenv("TMPDIR", tmp);
	std::set<char> delims{'/'};

	std::vector<std::string> path = splitpath(filename, delims);
	std::string basename =  path.back();
	std::string laz_filename = tmp+"/"+ basename + ".laz";
	pdal::Options writerOps;
    writerOps.add("filename", laz_filename);
    writerOps.add("compression", true);
	pdal::LasWriter writer;
	pdal::BufferReader breader;
	breader.addView(view);
    writer.setInput(breader);
    writer.setOptions(writerOps);
//
	pdal::PointTable table2;
    writer.prepare(table2);
	writer.execute(table2);
	return laz_filename;

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
    stats["buffer_size"] = (int)table.m_buf.size();
    stats["file_size"] = (int)getFileSize(filename);

	std::string laz_filename = writeLaz(filename, view);

    stats["laz_size"] = (int)getFileSize(laz_filename);
	std::remove(laz_filename.c_str());

    // check laz-perf
    start = now();
    std::vector<char> compressed = lazperf(table.m_buf, view, view->size());
    auto lazperf_time = microsecondsSince(start);
    Json::Value lazperf_stats;
    lazperf_stats["compression_time"] = lazperf_time;
    lazperf_stats["compressed_size"] =  (int)( compressed.size());

    // check lzma
    start = now();
    std::vector<int8_t> lzma_data = lzma(table.m_buf);
    auto lzma_time = microsecondsSince(start);
//
    Json::Value lzma_stats;
    lzma_stats["compression_time"] = lzma_time;
    lzma_stats["compression_size"] = (int)(lzma_data.size());
//
    json["lzma"] = lzma_stats;
    json["lazperf"] = lazperf_stats;
    json["stats"] = stats;
    json["Filename"] = filename;

    std::cout << json << std::endl;

    return 0;
}
