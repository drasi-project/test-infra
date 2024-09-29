use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct GdeltEvent {
    global_event_id: u64,         // GlobalEventID
    day: u32,                     // EventDay (e.g., 20230921)
    month_year: u32,              // EventMonthYear (e.g., 202309)
    year: u16,                    // EventYear (e.g., 2023)
    fraction_date: f64,           // FractionalDate (e.g., 2023.7151)
    actor1_code: Option<String>,  // Actor1Code (e.g., "AUS")
    actor1_name: Option<String>,  // Actor1Name (e.g., "AUSTRALIA")
    actor1_country_code: Option<String>,  // Actor1CountryCode (e.g., "AUS")
    actor1_known_group_code: Option<String>,  // Actor1KnownGroupCode
    actor1_ethnic_code: Option<String>,  // Actor1EthnicCode
    actor1_religion1_code: Option<String>,  // Actor1Religion1Code
    actor1_religion2_code: Option<String>,  // Actor1Religion2Code
    actor1_type1_code: Option<String>,  // Actor1Type1Code
    actor1_type2_code: Option<String>,  // Actor1Type2Code
    actor1_type3_code: Option<String>,  // Actor1Type3Code
    actor2_code: Option<String>,  // Actor2Code
    actor2_name: Option<String>,  // Actor2Name
    actor2_country_code: Option<String>,  // Actor2CountryCode
    actor2_known_group_code: Option<String>,  // Actor2KnownGroupCode
    actor2_ethnic_code: Option<String>,  // Actor2EthnicCode
    actor2_religion1_code: Option<String>,  // Actor2Religion1Code
    actor2_religion2_code: Option<String>,  // Actor2Religion2Code
    actor2_type1_code: Option<String>,  // Actor2Type1Code
    actor2_type2_code: Option<String>,  // Actor2Type2Code
    actor2_type3_code: Option<String>,  // Actor2Type3Code
    is_root_event: u8,            // IsRootEvent (0 or 1)
    event_code: String,           // EventCode (e.g., "037")
    event_base_code: String,      // EventBaseCode (e.g., "037")
    event_root_code: String,      // EventRootCode (e.g., "03")
    quad_class: u8,               // QuadClass (e.g., 1)
    goldstein_scale: f32,         // GoldsteinScale (e.g., 5.0)
    num_mentions: u32,            // NumMentions (e.g., 64)
    num_sources: u32,             // NumSources (e.g., 6)
    num_articles: u32,            // NumArticles (e.g., 64)
    avg_tone: f32,                // AvgTone (e.g., -8.3125)
    actor1_geo_type: Option<u8>,  // Actor1Geo_Type (e.g., 4)
    actor1_geo_full_name: Option<String>,  // Actor1Geo_FullName (e.g., "Perth, Western Australia, Australia")
    actor1_geo_country_code: Option<String>,  // Actor1Geo_CountryCode (e.g., "AS")
    actor1_geo_adm1_code: Option<String>,  // Actor1Geo_ADM1Code (e.g., "AS08")
    actor1_geo_adm2_code: Option<String>,  // Actor1Geo_ADM2Code (e.g., "08")
    actor1_geo_lat: Option<f64>,  // Actor1Geo_Lat (e.g., -31.9333)
    actor1_geo_long: Option<f64>,  // Actor1Geo_Long (e.g., 115.833)
    actor1_geo_feature_id: Option<String>,  // Actor1Geo_FeatureID (e.g., -1594675)
    actor2_geo_type: Option<u8>,  // Actor2Geo_Type (e.g., 4)
    actor2_geo_full_name: Option<String>,  // Actor2Geo_FullName (e.g., "Perth, Western Australia, Australia")
    actor2_geo_country_code: Option<String>,  // Actor2Geo_CountryCode
    actor2_geo_adm1_code: Option<String>,  // Actor2Geo_ADM1Code
    actor2_geo_adm2_code: Option<String>,  // Actor2Geo_ADM2Code
    actor2_geo_lat: Option<f64>,  // Actor2Geo_Lat
    actor2_geo_long: Option<f64>,  // Actor2Geo_Long
    actor2_geo_feature_id: Option<String>,  // Actor2Geo_FeatureID
    action_geo_type: Option<u8>,  // ActionGeo_Type (e.g., 4)
    action_geo_full_name: Option<String>,  // ActionGeo_FullName (e.g., "Perth, Western Australia, Australia")
    action_geo_country_code: Option<String>,  // ActionGeo_CountryCode (e.g., "AS")
    action_geo_adm1_code: Option<String>,  // ActionGeo_ADM1Code (e.g., "AS08")
    action_geo_adm2_code: Option<String>,  // ActionGeo_ADM2Code (e.g., "08")
    action_geo_lat: Option<f64>,  // ActionGeo_Lat (e.g., -31.9333)
    action_geo_long: Option<f64>,  // ActionGeo_Long (e.g., 115.833)
    action_geo_feature_id: Option<String>,  // ActionGeo_FeatureID (e.g., -1594675)
    date_added: u64,           // DateAdded (e.g., "20240920200000")
    source_url: Option<String>,   // SourceURL (e.g., "https://example.com")
}

#[derive(Debug, Deserialize)]
pub struct GdeltMention {
    global_event_id: u64,         // GlobalEventID
    event_time_date: u64,         // EventTimeDate (e.g., 20240920164500). Event.date_added
    mention_time_date: u64,       // MentionTimeDate (e.g., 20240920200000)
    mention_type: u8,             // MentionType (e.g., 1)
    mention_source_name: String,  // MentionSourceName
    mention_identifier: String,   // MentionIdentifier (e.g., URL)
    sentence_id: u32,             // SentenceID (e.g., 1)
    actor1_char_offset: i32,      // Actor1CharOffset (e.g., 0)
    actor2_char_offset: i32,      // Actor2CharOffset (e.g., 0)
    action_char_offset: i32,      // ActionCharOffset (e.g., 0)
    in_raw_text: String,          // InRawText (e.g., "Perth is the capital of Western Australia.")
    confidence: i32,              // Confidence (e.g., 100)  
    mention_doc_len: i32,         // MentionDocLen (e.g., 5)
    mention_doc_tone: f32,        // MentionDocTone (e.g., 1935)
    mention_doc_translation_info: String,  // MentionDocTranslationInfo (e.g., "eng")
    mention_doc_extra: String,    // MentionDocExtra (e.g., "extra")
}

#[derive(Debug, Deserialize)]
pub struct GdeltGraph {
    record_id: String,         // GKG Record ID
    v21_date: u64,             // Date of the GKG record
    v2_source_collection_identifier: String,  // Source Collection Identifier
    v2_source_common_name: String,  // Source Common Name
    v2_document_identifier: String,  // Document Identifier
    v1_counts: String,             // Event counts and other relevant metrics
    v21_counts: String,            // Counter
    v1_themes: String,          // Location related data
    v2_themes: String,  // Country of the location
    v1_locations: String,       // Source URL
    v2_locations: String,  // Source of the article
    v1_persons: String,         // Type of event
    v2_persons: String,  // Article's title
    v1_organizations: String,   // Article's content
    v2_organizations: String,  // Keywords
    v1_tone: String,            // Themes
    v21_dates: String,  // Quotations
    v2_gcam: String,           // Persons mentioned
    v21_sharing_image: String,     // Organizations mentioned
    v21_related_images: String, // Edition of the article
    v21_social_image_embeds: String,  // Language of the source
    v21_social_video_embeds: String,  // Language of the source
    v21_quotations: String,     // Language of the source
    v21_all_names: String,       // Language of the source
    v21_amounts: String,        // Language of the source
    v21_translation_info: String,  // Language of the source
    v2_extras_xml: String,     // Language of the source
}