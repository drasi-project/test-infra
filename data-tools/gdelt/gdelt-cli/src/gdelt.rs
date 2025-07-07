// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct GdeltEvent {
    pub global_event_id: i64,         // GlobalEventID
    pub day: i32,                     // EventDay (e.g., 20230921)
    pub month_year: i32,              // EventMonthYear (e.g., 202309)
    pub year: i32,                    // EventYear (e.g., 2023)
    pub fraction_date: f64,           // FractionalDate (e.g., 2023.7151)
    pub actor1_code: Option<String>,  // Actor1Code (e.g., "AUS")
    pub actor1_name: Option<String>,  // Actor1Name (e.g., "AUSTRALIA")
    pub actor1_country_code: Option<String>,  // Actor1CountryCode (e.g., "AUS")
    pub actor1_known_group_code: Option<String>,  // Actor1KnownGroupCode
    pub actor1_ethnic_code: Option<String>,  // Actor1EthnicCode
    pub actor1_religion1_code: Option<String>,  // Actor1Religion1Code
    pub actor1_religion2_code: Option<String>,  // Actor1Religion2Code
    pub actor1_type1_code: Option<String>,  // Actor1Type1Code
    pub actor1_type2_code: Option<String>,  // Actor1Type2Code
    pub actor1_type3_code: Option<String>,  // Actor1Type3Code
    pub actor2_code: Option<String>,  // Actor2Code
    pub actor2_name: Option<String>,  // Actor2Name
    pub actor2_country_code: Option<String>,  // Actor2CountryCode
    pub actor2_known_group_code: Option<String>,  // Actor2KnownGroupCode
    pub actor2_ethnic_code: Option<String>,  // Actor2EthnicCode
    pub actor2_religion1_code: Option<String>,  // Actor2Religion1Code
    pub actor2_religion2_code: Option<String>,  // Actor2Religion2Code
    pub actor2_type1_code: Option<String>,  // Actor2Type1Code
    pub actor2_type2_code: Option<String>,  // Actor2Type2Code
    pub actor2_type3_code: Option<String>,  // Actor2Type3Code
    pub is_root_event: i32,            // IsRootEvent (0 or 1)
    pub event_code: Option<String>,           // EventCode (e.g., "037")
    pub event_base_code: Option<String>,      // EventBaseCode (e.g., "037")
    pub event_root_code: Option<String>,      // EventRootCode (e.g., "03")
    pub quad_class: i32,               // QuadClass (e.g., 1)
    pub goldstein_scale: f32,         // GoldsteinScale (e.g., 5.0)
    pub num_mentions: i32,            // NumMentions (e.g., 64)
    pub num_sources: i32,             // NumSources (e.g., 6)
    pub num_articles: i32,            // NumArticles (e.g., 64)
    pub avg_tone: f32,                // AvgTone (e.g., -8.3125)
    pub actor1_geo_type: Option<i32>,  // Actor1Geo_Type (e.g., 4)
    pub actor1_geo_full_name: Option<String>,  // Actor1Geo_FullName (e.g., "Perth, Western Australia, Australia")
    pub actor1_geo_country_code: Option<String>,  // Actor1Geo_CountryCode (e.g., "AS")
    pub actor1_geo_adm1_code: Option<String>,  // Actor1Geo_ADM1Code (e.g., "AS08")
    pub actor1_geo_adm2_code: Option<String>,  // Actor1Geo_ADM2Code (e.g., "08")
    pub actor1_geo_lat: Option<f64>,  // Actor1Geo_Lat (e.g., -31.9333)
    pub actor1_geo_long: Option<f64>,  // Actor1Geo_Long (e.g., 115.833)
    pub actor1_geo_feature_id: Option<String>,  // Actor1Geo_FeatureID (e.g., -1594675)
    pub actor2_geo_type: Option<i32>,  // Actor2Geo_Type (e.g., 4)
    pub actor2_geo_full_name: Option<String>,  // Actor2Geo_FullName (e.g., "Perth, Western Australia, Australia")
    pub actor2_geo_country_code: Option<String>,  // Actor2Geo_CountryCode
    pub actor2_geo_adm1_code: Option<String>,  // Actor2Geo_ADM1Code
    pub actor2_geo_adm2_code: Option<String>,  // Actor2Geo_ADM2Code
    pub actor2_geo_lat: Option<f64>,  // Actor2Geo_Lat
    pub actor2_geo_long: Option<f64>,  // Actor2Geo_Long
    pub actor2_geo_feature_id: Option<String>,  // Actor2Geo_FeatureID
    pub action_geo_type: Option<i32>,  // ActionGeo_Type (e.g., 4)
    pub action_geo_full_name: Option<String>,  // ActionGeo_FullName (e.g., "Perth, Western Australia, Australia")
    pub action_geo_country_code: Option<String>,  // ActionGeo_CountryCode (e.g., "AS")
    pub action_geo_adm1_code: Option<String>,  // ActionGeo_ADM1Code (e.g., "AS08")
    pub action_geo_adm2_code: Option<String>,  // ActionGeo_ADM2Code (e.g., "08")
    pub action_geo_lat: Option<f64>,  // ActionGeo_Lat (e.g., -31.9333)
    pub action_geo_long: Option<f64>,  // ActionGeo_Long (e.g., 115.833)
    pub action_geo_feature_id: Option<String>,  // ActionGeo_FeatureID (e.g., -1594675)
    pub date_added: i64,           // DateAdded (e.g., "20240920200000")
    pub source_url: Option<String>,   // SourceURL (e.g., "https://example.com")
}

#[derive(Debug, Deserialize)]
pub struct GdeltMention {
    pub global_event_id: i64,         // GlobalEventID
    pub event_time_date: i64,         // EventTimeDate (e.g., 20240920164500). Event.date_added
    pub mention_time_date: i64,       // MentionTimeDate (e.g., 20240920200000)
    pub mention_type: i32,             // MentionType (e.g., 1)
    pub mention_source_name: String,  // MentionSourceName
    pub mention_identifier: String,   // MentionIdentifier (e.g., URL)
    pub sentence_id: i32,             // SentenceID (e.g., 1)
    pub actor1_char_offset: i32,      // Actor1CharOffset (e.g., 0)
    pub actor2_char_offset: i32,      // Actor2CharOffset (e.g., 0)
    pub action_char_offset: i32,      // ActionCharOffset (e.g., 0)
    pub in_raw_text: String,          // InRawText (e.g., "Perth is the capital of Western Australia.")
    pub confidence: i32,              // Confidence (e.g., 100)  
    pub mention_doc_len: i32,         // MentionDocLen (e.g., 5)
    pub mention_doc_tone: f32,        // MentionDocTone (e.g., 1935)
    pub mention_doc_translation_info: String,  // MentionDocTranslationInfo (e.g., "eng")
    pub mention_doc_extra: String,    // MentionDocExtra (e.g., "extra")
}

#[derive(Debug, Deserialize)]
pub struct GdeltGraph {
    pub record_id: String,         // GKG Record ID
    pub v21_date: i64,             // Date of the GKG record
    pub v2_source_collection_identifier: String,  // Source Collection Identifier
    pub v2_source_common_name: String,  // Source Common Name
    pub v2_document_identifier: String,  // Document Identifier
    pub v1_counts: String,             // Event counts and other relevant metrics
    pub v21_counts: String,            // Counter
    pub v1_themes: String,          // Location related data
    pub v2_themes: String,  // Country of the location
    pub v1_locations: String,       // Source URL
    pub v2_locations: String,  // Source of the article
    pub v1_persons: String,         // Type of event
    pub v2_persons: String,  // Article's title
    pub v1_organizations: String,   // Article's content
    pub v2_organizations: String,  // Keywords
    pub v1_tone: String,            // Themes
    pub v21_dates: String,  // Quotations
    pub v2_gcam: String,           // Persons mentioned
    pub v21_sharing_image: String,     // Organizations mentioned
    pub v21_related_images: String, // Edition of the article
    pub v21_social_image_embeds: String,  // Language of the source
    pub v21_social_video_embeds: String,  // Language of the source
    pub v21_quotations: String,     // Language of the source
    pub v21_all_names: String,       // Language of the source
    pub v21_amounts: String,        // Language of the source
    pub v21_translation_info: String,  // Language of the source
    pub v2_extras_xml: String,     // Language of the source
}