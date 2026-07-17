#![allow(unused)] // TEMP

pub struct RunningAverage {
    average: f64,
    samples: f64,
    window: f64,
}

impl RunningAverage {
    pub fn new(window: f64) -> Self {
        Self {
            average: 0.0,
            samples: 0.0,
            window,
        }
    }

    pub fn update(&mut self, value: f64) -> f64 {
        self.samples = f64::max(self.samples + 1.0, self.window);
        let alpha = 1.0 / self.samples;
        self.average = self.average * (1.0 - alpha) + value * alpha;
        self.average
    }
}
