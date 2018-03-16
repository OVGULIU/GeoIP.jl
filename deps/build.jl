using CSV
using SQLite
using ZipFile
using Requests

# Schema for reading csv tables
const locale_schema = [Int, String, String, String, String, String, String, String, String, String, String, Int, String, Int]
const block_schema = [String, Int, Int, String, Int, Int, String, Float64, Float64, Int]

# URL for data
const URL = "http://geolite.maxmind.com/download/geoip/database/GeoLite2-City-CSV.zip"
const MD5 = "http://geolite.maxmind.com/download/geoip/database/GeoLite2-City-CSV.zip.md5"


function csvtotable(csv, db, table, schema)
    # Stream csv into a sqlite table
    SQLite.execute!(db, "DROP TABLE IF EXISTS $table")
    source = CSV.Source(csv, nullable=true, types=schema)
    CSV.read(source, SQLite.Sink, db, table)
end

function asdict(array)
    return Dict(value.name => value for value in array)
end

function tune!(db::SQLite.DB)
    SQLite.execute!(db, "PRAGMA synchronous = OFF")
    SQLite.execute!(db, "PRAGMA journal_mode = OFF")
    SQLite.execute!(db, "PRAGMA temp_store = MEMORY")
end

function main()
    download(URL, "geolite2.zip")
    download(MD5, "geolite2.md5")

    db = SQLite.DB("geolite2.db")
    tune!(db)

    r = try
        Requests.get(URL)
    catch
        error("Failed to download Geolite database, check network connectivity")
    end

    archive = ZipFile.Reader("geolite2.zip")
    dict = Dict(split(value.name, '/')[end] => value for value in archive.files)

    info("Inserting IPv4 Block data...")
    @time csvtotable(dict["GeoLite2-City-Blocks-IPv4.csv"], db, "blocks_ipv4", block_schema)

    # info("Inserting IPv6 Block data...")
    # csvtotable(dict["GeoLite2-City-Blocks-IPv6.csv"], db, "blocks_ipv6", block_schema)
    #
    # for locale in ["en", "de", "es", "fr", "ja", "pt-BR", "ru", "zh-CN"]
    #     info("Inserting locale $locale...")
    #     table_safe_locale = replace(locale, '-', '_')
    #     table = "locations_$table_safe_locale"
    #     csvtotable(dict["GeoLite2-City-Locations-$locale.csv"], db, table, locale_schema)
    # end


end

main()
