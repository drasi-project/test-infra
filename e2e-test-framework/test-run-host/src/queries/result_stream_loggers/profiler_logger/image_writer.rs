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

use std::{cmp::max, path::PathBuf};

use image::{Rgb, RgbImage};

use super::ChangeRecordProfile;

const PROFILE_COLORS: [Rgb<u8>; 10] = [
    Rgb([230, 25, 75]),    // Red - reactivator
    Rgb([60, 180, 75]),    // Green - source change queue
    Rgb([255, 225, 25]),   // Yellow - source change router
    Rgb([0, 130, 200]),    // Blue - source dispatch queue
    Rgb([245, 130, 48]),   // Orange - source change dispatcher
    Rgb([145, 30, 180]),   // Purple - query change queue
    Rgb([70, 240, 240]),   // Cyan - query host
    Rgb([240, 50, 230]),   // Magenta - query solver
    Rgb([210, 245, 60]),   // Lime - result queue
    Rgb([128, 128, 128]),  // Gray - shortfall
];

#[allow(dead_code)]
pub struct ProfileImageWriter {
    all_file_abs_path: PathBuf,
    all_file_rel_path: PathBuf,
    drasi_only_file_abs_path: PathBuf,
    drasi_only_file_rel_path: PathBuf,
    image_times: Vec<u64>,
    max_total_time: u64,
    max_drasi_only_time: u64,
    record_count: usize,
    width: u32,
}

impl ProfileImageWriter {
    pub async fn new(folder_path: PathBuf, file_name: String, width: u32) -> anyhow::Result<Self> {

        Ok(Self {
            all_file_abs_path: folder_path.join(format!("{}_all_abs.png", file_name)),
            all_file_rel_path: folder_path.join(format!("{}_all_rel.png", file_name)),
            drasi_only_file_abs_path: folder_path.join(format!("{}_drasi_only_abs.png", file_name)),
            drasi_only_file_rel_path: folder_path.join(format!("{}_drasi_only_rel.png", file_name)),
            image_times: Vec::new(),
            max_total_time: 0,
            max_drasi_only_time: 0,
            record_count: 0,
            width
        })
    }

    pub async fn add_change_profile(&mut self, profile: &ChangeRecordProfile) -> anyhow::Result<()> {
        
        let mut times = [
            profile.time_in_reactivator,
            profile.time_in_src_change_q,
            profile.time_in_src_change_rtr,
            profile.time_in_src_disp_q,
            profile.time_in_src_change_disp,
            profile.time_in_query_change_q,
            profile.time_in_query_host,
            profile.time_in_query_solver,
            profile.time_in_result_q,
            0,  // shortfall
            profile.time_total,
            0   // total for drasi only components
        ];

        let drasi_sum = times[0] + times[2] + times[4] + times[6] + times[7];
        let all_sum = drasi_sum + times[1] + times[3] + times[5] + times[8];

        times[9] = times[10] - all_sum;
        times[11] = drasi_sum;

        self.max_total_time = max(self.max_total_time, profile.time_total);
        self.max_drasi_only_time = max(self.max_drasi_only_time, drasi_sum);

        self.image_times.extend(&times);

        self.record_count += 1;

        Ok(())
    }

    pub async fn generate_image(&self) -> anyhow::Result<()> {
        self.generate_all_image().await?;
        self.generate_drasi_only_image().await?;
        Ok(())
    }

    async fn generate_all_image(&self) -> anyhow::Result<()> {

        let header_height: u32 = 20;
        let header_span_width = self.width / PROFILE_COLORS.len() as u32; 
        let height = self.record_count as u32 + header_height;
        let times_per_profile: usize = 12;
        let mut img_abs = RgbImage::new(self.width, height);
        let mut img_rel = RgbImage::new(self.width, height);

        // Draw the header (equal-length spans for each color)
        for y in 0..header_height {
            let mut x = 0;
            for &color in &PROFILE_COLORS {
                for px in x..x + header_span_width {
                    if px < self.width {
                        img_abs.put_pixel(px, y, color);
                        img_rel.put_pixel(px, y, color);
                    }
                }
                x += header_span_width;
            }
        }

        // Draw the image from the spans
        self.image_times
            .chunks(times_per_profile)
            .enumerate()
            .for_each(|(y, raw_times)| {

                // Absolute
                let mut x = 0;
                let mut pixels_per_unit = self.width as f64 / self.max_total_time as f64;
                let mut span_width: u32;
                for i in 0..10 {
                    if raw_times[i] > 0 {
                        span_width = (raw_times[i] as f64 * pixels_per_unit).round() as u32;

                        if span_width > 0 {
                            for px in x..x + span_width {
                                if px < self.width {
                                    img_abs.put_pixel(px, y as u32 + header_height, PROFILE_COLORS[i]);
                                }
                            }
                            x += span_width;
                        }
                    };
                }

                // Relative
                x = 0;
                pixels_per_unit = self.width as f64 / raw_times[10] as f64;
                for i in 0..10 {
                    if raw_times[i] > 0 {                        
                        span_width = (raw_times[i] as f64 * pixels_per_unit).round() as u32;

                        if span_width > 0 {
                            for px in x..x + span_width {
                                if px < self.width {
                                    img_rel.put_pixel(px, y as u32 + header_height, PROFILE_COLORS[i]);
                                }
                            }
                            x += span_width;
                        }
                    };
                }
            });

        // Save the image
        img_abs.save(&self.all_file_abs_path)?;
        img_rel.save(&self.all_file_rel_path)?;

        Ok(())
    }

    async fn generate_drasi_only_image(&self) -> anyhow::Result<()> {

        let header_height: u32 = 20;
        let header_span_width = self.width / PROFILE_COLORS.len() as u32; 
        let height = self.record_count as u32 + header_height;
        let times_per_profile: usize = 12;
        let mut img_abs = RgbImage::new(self.width, height);
        let mut img_rel = RgbImage::new(self.width, height);

        // Draw the header (equal-length spans for each color)
        for y in 0..header_height {
            let mut x = 0;
            for &color in &PROFILE_COLORS {
                for px in x..x + header_span_width {
                    if px < self.width {
                        img_abs.put_pixel(px, y, color);
                        img_rel.put_pixel(px, y, color);
                    }
                }
                x += header_span_width;
            }
        }

        // Draw the image from the spans
        self.image_times
            .chunks(times_per_profile)
            .enumerate()
            .for_each(|(y, raw_times)| {

                // Absolute
                let mut x = 0;
                let mut pixels_per_unit = self.width as f64 / self.max_drasi_only_time as f64;
                let mut span_width: u32;
                for i in [0, 2, 4, 6, 7] {
                    if raw_times[i] > 0 {
                        span_width = (raw_times[i] as f64 * pixels_per_unit).round() as u32;

                        if span_width > 0 {
                            for px in x..x + span_width {
                                if px < self.width {
                                    img_abs.put_pixel(px, y as u32 + header_height, PROFILE_COLORS[i]);
                                }
                            }
                            x += span_width;
                        }
                    };
                }

                // Relative
                x = 0;
                pixels_per_unit = self.width as f64 / raw_times[11] as f64;
                for i in [0, 2, 4, 6, 7] {
                    if raw_times[i] > 0 {                        
                        span_width = (raw_times[i] as f64 * pixels_per_unit).round() as u32;

                        if span_width > 0 {
                            for px in x..x + span_width {
                                if px < self.width {
                                    img_rel.put_pixel(px, y as u32 + header_height, PROFILE_COLORS[i]);
                                }
                            }
                            x += span_width;
                        }
                    };
                }
            });

        // Save the image
        img_abs.save(&self.drasi_only_file_abs_path)?;
        img_rel.save(&self.drasi_only_file_rel_path)?;

        Ok(())
    }
}