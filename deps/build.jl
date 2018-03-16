import CSV
import SQLite
import InfoZIP
import Glob

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
    const zipfile = "geolite2.zip"
    const md5 = "geolite2.md5"

    download(URL, zipfile)
    download(MD5, md5)
    InfoZIP.unzip(zipfile)

    db = SQLite.DB("geolite2.db")
    tune!(db)

    files = Glob.glob("GeoLite2-City*/*.csv")
    dict = Dict(split(path, '/')[end] => path for path in files)

    info("Inserting IPv4 Block data...")
    csvtotable(dict["GeoLite2-City-Blocks-IPv4.csv"], db, "blocks_ipv4", block_schema)

    info("Inserting IPv6 Block data...")
    csvtotable(dict["GeoLite2-City-Blocks-IPv6.csv"], db, "blocks_ipv6", block_schema)

    for locale in ["en", "de", "es", "fr", "ja", "pt-BR", "ru", "zh-CN"]
        info("Inserting locale $locale...")
        table_safe_locale = replace(locale, '-', '_')
        table = "locations_$table_safe_locale"
        csvtotable(dict["GeoLite2-City-Locations-$locale.csv"], db, table, locale_schema)
    end

    dir = Glob.glob("GeoLite2-City*")[1]
    rm(dir, force=true, recursive=true)
    rm(zipfile)
end

main()
