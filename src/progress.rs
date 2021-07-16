use indicatif::HumanDuration;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;

pub struct ProgressIndicator {
    disabled: bool,
    pb: ProgressBar,
    file_count: u64,
}

impl ProgressIndicator {
    pub fn new(disabled: bool) -> Self {
        ProgressIndicator {
            disabled,
            pb: ProgressBar::new(0),
            file_count: 0,
        }
    }

    pub fn scan_begin(&mut self) {
        if self.disabled {
            return;
        }
        self.pb = ProgressBar::new_spinner();
        self.file_count = 0;
    }

    pub fn scan_progress(&mut self, s: &str) {
        if self.disabled {
            return;
        }
        self.pb.set_message(format!("Scanning... {}", s));
        self.file_count += 1;
    }

    pub fn scan_end(&self) {
        if self.disabled {
            return;
        }
        let summary = format!(
            "Scanned {} files in {}",
            self.file_count,
            HumanDuration(self.pb.elapsed())
        );
        self.pb.finish_with_message(summary);
    }

    pub fn hash_begin(&mut self, total_bytes: u64) {
        if self.disabled {
            return;
        }
        self.pb = ProgressBar::new(total_bytes);
        self.pb.set_position(0);
        self.pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{elapsed_precise}] [{wide_bar:.cyan/blue}] \
                     {bytes}/{total_bytes} {bytes_per_sec} (ETA {eta_precise})",
                )
                .progress_chars("#>-"),
        );
    }

    pub fn hash_progress(&mut self, processed_bytes: u64) {
        if self.disabled {
            return;
        }
        if processed_bytes == 0 {
            return;
        }
        self.pb.inc(processed_bytes);
    }

    pub fn hash_end(&self) {
        if self.disabled {
            return;
        }
        self.pb.finish_with_message("Hash complete.");
    }
}
