//! Keeps the embedded terminals in the colors of the active Maruzzella
//! palette, including live palette switches.

use std::cell::RefCell;

use gtk::gdk::RGBA;
use gtk::glib;
use maruzzella_sdk::ffi::{MzBytes, MzStatus};
use maruzzella_sdk::{theme_palette_from_event, HostApi, MzThemePalette};
use vte4::prelude::*;

thread_local! {
    static PALETTE: RefCell<Option<MzThemePalette>> = const { RefCell::new(None) };
    static TERMINALS: RefCell<Vec<glib::WeakRef<vte4::Terminal>>> = const { RefCell::new(Vec::new()) };
}

/// Reads the current palette and subscribes to palette switches. Must run
/// during plugin registration, where host event subscriptions are accepted.
pub(crate) fn register(host: &HostApi<'_>) {
    if let Ok(Some(palette)) = host.read_theme_palette() {
        PALETTE.with(|current| current.replace(Some(palette)));
    }
    // Hosts without palette events simply leave terminals in the palette
    // read at startup.
    let _ = host.subscribe_theme_palette_changed(on_palette_changed);
}

extern "C" fn on_palette_changed(event: MzBytes) -> MzStatus {
    if let Some(palette) = theme_palette_from_event(event) {
        PALETTE.with(|current| current.replace(Some(palette)));
        TERMINALS.with(|terminals| {
            terminals
                .borrow_mut()
                .retain(|terminal| match terminal.upgrade() {
                    Some(terminal) => {
                        apply_current(&terminal);
                        true
                    }
                    None => false,
                });
        });
    }
    MzStatus::OK
}

/// Colors `terminal` with the current palette and keeps it in sync with
/// future palette switches.
pub(crate) fn attach(terminal: &vte4::Terminal) {
    apply_current(terminal);
    TERMINALS.with(|terminals| {
        let mut terminals = terminals.borrow_mut();
        terminals.retain(|terminal| terminal.upgrade().is_some());
        terminals.push(terminal.downgrade());
    });
}

fn apply_current(terminal: &vte4::Terminal) {
    PALETTE.with(|current| {
        if let Some(palette) = current.borrow().as_ref() {
            apply(terminal, &TerminalColors::from_palette(palette));
        }
    });
}

fn apply(terminal: &vte4::Terminal, colors: &TerminalColors) {
    let rgba = |color: &str| RGBA::parse(color).ok();
    // An empty or unparsable set keeps VTE's own ANSI colors (older hosts
    // send none).
    let ansi = colors
        .ansi
        .iter()
        .map(|color| rgba(color))
        .collect::<Option<Vec<_>>>()
        .filter(|ansi| ansi.len() == 16)
        .unwrap_or_default();
    let ansi = ansi.iter().collect::<Vec<_>>();
    terminal.set_colors(
        rgba(&colors.foreground).as_ref(),
        rgba(&colors.background).as_ref(),
        &ansi,
    );
    terminal.set_color_cursor(rgba(&colors.cursor).as_ref());
    terminal.set_color_cursor_foreground(rgba(&colors.background).as_ref());
    terminal.set_color_highlight(rgba(&colors.selection_bg).as_ref());
    terminal.set_color_highlight_foreground(rgba(&colors.selection_text).as_ref());
}

#[derive(Debug, PartialEq, Eq)]
struct TerminalColors {
    foreground: String,
    background: String,
    cursor: String,
    selection_bg: String,
    selection_text: String,
    ansi: Vec<String>,
}

impl TerminalColors {
    fn from_palette(palette: &MzThemePalette) -> Self {
        Self {
            foreground: palette.text_0.clone(),
            background: palette.workbench.clone(),
            cursor: palette.text_0.clone(),
            selection_bg: palette.selection_bg.clone(),
            selection_text: palette.selection_text.clone(),
            ansi: palette.terminal_ansi.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_colors_follow_the_palette() {
        let ansi = (0..16)
            .map(|index| format!("#0000{index:02x}"))
            .collect::<Vec<_>>();
        let palette = MzThemePalette {
            is_light: true,
            workbench: "#ffffff".to_string(),
            text_0: "#1e1f22".to_string(),
            selection_bg: "#d4e2ff".to_string(),
            selection_text: "#1e1f22".to_string(),
            terminal_ansi: ansi.clone(),
            ..MzThemePalette::default()
        };
        let colors = TerminalColors::from_palette(&palette);
        assert_eq!(colors.background, "#ffffff");
        assert_eq!(colors.foreground, "#1e1f22");
        assert_eq!(colors.selection_bg, "#d4e2ff");
        assert_eq!(colors.ansi, ansi);
    }
}
