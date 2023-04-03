const SQLITE = require("sqlite");
const SCHEMA = {"data": {"columns": ["k", "v"], "index": [], "primary": "k" }}

class KVF
{
	constructor(path, is_binary)
	{
		var _this = this;
		this.is_binary = is_binary;
		if (typeof this.is_binary == "undefined")
			this.is_binary = false;

		this.db = new SQLITE(path, SCHEMA);
	}

	set(k, v)
	{
		var params = [k];
		
		if (!this.is_binary) params.push(JSON.stringify(v));
		else params.push(v);

		this.db.fast_write("REPLACE INTO data (k,v) VALUES (?,?)", params);
	}

	list_keys(prefix)
	{
		var list = [];
		var rows = this.db.read("SELECT k FROM data WHERE k LIKE (? || '%')", prefix);
		for(var key in this.cache)
		{
			if (key.startsWith(prefix))
				list.push(key);
		}

		for(var i=0;i<rows.length;i++)
			list.push(rows[i]["k"]);

		return Array.from(new Set(list));
	}

	get(k, default_value)
	{
		var v = this.db.read("SELECT v FROM data WHERE k = ?", k)[0];
		
		if (typeof v == "undefined")
			return default_value;

		v = v["v"];
		if (!this.is_binary)
			v = JSON.parse(v);

		return v;
	}

	del(k)
	{
		this.db.commit();
		this.db.write("DELETE FROM data WHERE k = ?", k);
	}

	flush() { this.db.commit(); }
	// these are left blank for API compatilbity with KV
	remove_infrequent_keys() {}
}

module.exports = KVF;
