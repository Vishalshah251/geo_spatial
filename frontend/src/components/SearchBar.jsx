import React from "react";

export default function SearchBar({ value, onChange, placeholder = "Search POIs..." }) {
  return (
    <div className="search">
      <input
        className="searchInput"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        spellCheck={false}
      />
      {value ? (
        <button className="searchClear" type="button" onClick={() => onChange("")}>
          Clear
        </button>
      ) : null}
    </div>
  );
}

