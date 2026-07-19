//! Deterministic visual gallery for agent status primitives.
//!
//! Review snapshots with `just test -p codex-tui agent_status_gallery`, then inspect pending files
//! with `cargo insta pending-snapshots -p codex-tui` and accept intentionally changed snapshots
//! with `cargo insta accept -p codex-tui`. For PTY review, run Codex with `--no-alt-screen`, open
//! `/agent`, and capture wide/narrow panes with `tmux capture-pane -p -e` against mocked events.

use super::super::history_cell::ReportedChecklistStatus;
use super::super::history_cell::ReportedChecklistStep;
use super::super::history_cell::render_reported_checklist;
use super::super::wrapping::RtOptions;
use super::super::wrapping::adaptive_wrap_line;
use crate::test_backend::VT100Backend;
use ratatui::backend::TestBackend;
use ratatui::prelude::*;
use ratatui::style::Styled;
use ratatui::widgets::Paragraph;
use ratatui::widgets::Wrap;
use unicode_width::UnicodeWidthStr;

const SIDE_CONTENT_BREAKPOINT: u16 = 40;
const FIXED_OBSERVED_AT: &str = "2026-07-19T12:34:56Z";
const PRICING_VERSION: &str = "pricing-v1";

fn status_line(width: u16, text: &str, style: Style) -> Vec<Line<'static>> {
    let line = Line::from(text.to_string().set_style(style));
    adaptive_wrap_line(&line, RtOptions::new(width.max(1) as usize))
        .iter()
        .map(crate::render::line_utils::line_to_static)
        .collect()
}

fn full_gallery(width: u16) -> Vec<Line<'static>> {
    let mut lines = status_line(
        width,
        &format!("Agent status gallery · observed {FIXED_OBSERVED_AT} · {PRICING_VERSION}"),
        Style::default().bold(),
    );
    let rows = [
        (
            "› └─ root",
            "working · checklist 2/4",
            Style::default().cyan().bold(),
        ),
        (
            "  ├─ research/boe",
            "waiting · checklist 3/5",
            Style::default().cyan(),
        ),
        (
            "  │ ├─ tui",
            "finished · checklist 4/4",
            Style::default().dim(),
        ),
        (
            "  │ └─ protocol",
            "failed · closed · checklist 1/3",
            Style::default().red(),
        ),
        (
            "  ├─ review",
            "needs approval · no checklist observed",
            Style::default().cyan(),
        ),
        (
            "  └─ orphan",
            "status unavailable · parent unknown",
            Style::default().magenta(),
        ),
        (
            "  └─ deep/branch/task",
            "working · +7 more agents",
            Style::default().cyan(),
        ),
    ];
    for (owner, status, style) in rows {
        lines.extend(status_line(width, &format!("{owner:<22} {status}"), style));
    }

    lines.extend(status_line(
        width,
        "  selected · /root/research/boe/tui · Terra/high",
        Style::default().dim(),
    ));
    lines.extend(render_reported_checklist(
        width.saturating_sub(4),
        Some("reported checklist · exact turn turn-0002"),
        &[
            ReportedChecklistStep {
                step: "Compare published instruments 你好😀",
                status: ReportedChecklistStatus::Completed,
            },
            ReportedChecklistStep {
                step: "Trace event flow and preserve reconnect truth",
                status: ReportedChecklistStatus::InProgress,
            },
            ReportedChecklistStep {
                step: "Add deterministic snapshots",
                status: ReportedChecklistStatus::Pending,
            },
        ],
    ));
    lines.extend(status_line(
        width,
        "  tokens unavailable · est. cost unavailable",
        Style::default().dim(),
    ));
    lines
}

fn compact_gallery(width: u16) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(
        "Agent gallery · compact · reported state".bold(),
    )];
    for (owner, status, style) in [
        ("› root", "working", Style::default().cyan().bold()),
        ("  ├─ tui", "finished", Style::default().dim()),
        ("  ├─ review", "failed · closed", Style::default().red()),
        (
            "  └─ orphan",
            "status unavailable",
            Style::default().magenta(),
        ),
        ("  └─ nested", "needs input", Style::default().cyan()),
    ] {
        lines.extend(status_line(width, &format!("{owner:<14} {status}"), style));
    }
    lines.push(Line::from("  ↑↓ select · Enter watch · Esc close".dim()));
    lines
}

fn render_test_backend(width: u16, height: u16, lines: Vec<Line<'static>>) -> String {
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).expect("test backend terminal");
    terminal
        .draw(|frame| {
            frame.render_widget(
                Paragraph::new(lines).wrap(Wrap { trim: false }),
                frame.area(),
            );
        })
        .expect("draw gallery");
    let buffer = terminal.backend().buffer();
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buffer[(x, y)].symbol().to_string())
                .collect::<String>()
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_vt100_backend(width: u16, height: u16, lines: Vec<Line<'static>>) -> String {
    let backend = VT100Backend::new(width, height);
    let mut terminal = Terminal::new(backend).expect("vt100 backend terminal");
    terminal
        .draw(|frame| {
            frame.render_widget(
                Paragraph::new(lines).wrap(Wrap { trim: false }),
                frame.area(),
            );
        })
        .expect("draw gallery");
    terminal.backend().to_string()
}

#[test]
fn agent_status_gallery_test_backend_snapshots() {
    for width in [40, 80, 120] {
        let lines = full_gallery(width);
        for line in &lines {
            let text = line
                .spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect::<String>();
            assert!(
                text.width() <= width as usize,
                "gallery line exceeds {width} columns: {text:?}"
            );
        }
        let rendered = render_test_backend(width, lines.len() as u16, lines.clone());
        assert!(rendered.contains("research/boe"));
        assert!(rendered.contains('你'));
        assert!(rendered.contains('好'));
        assert!(rendered.contains('😀'));
        insta::assert_snapshot!(
            format!("agent_status_gallery_test_backend_{width}"),
            rendered
        );
    }
}

#[test]
fn agent_status_gallery_breakpoint_vt100_snapshots() {
    for width in [
        SIDE_CONTENT_BREAKPOINT - 1,
        SIDE_CONTENT_BREAKPOINT,
        SIDE_CONTENT_BREAKPOINT + 1,
    ] {
        let lines = compact_gallery(width);
        let rendered = render_vt100_backend(width, 12, lines);
        assert!(rendered.contains("root"));
        insta::assert_snapshot!(format!("agent_status_gallery_vt100_{width}"), rendered);
    }
}

#[test]
fn agent_status_gallery_asserts_selected_and_completed_cell_styles() {
    let width = 80;
    let lines = full_gallery(width);
    let backend = TestBackend::new(width, lines.len() as u16);
    let mut terminal = Terminal::new(backend).expect("test backend terminal");
    terminal
        .draw(|frame| frame.render_widget(Paragraph::new(lines), frame.area()))
        .expect("draw gallery");
    let buffer = terminal.backend().buffer();
    let selected = buffer
        .content
        .iter()
        .find(|cell| cell.symbol() == "›")
        .expect("selected marker");
    assert!(selected.style().fg == Some(Color::Cyan));
    assert!(selected.style().add_modifier.contains(Modifier::BOLD));
    let completed = buffer
        .content
        .iter()
        .find(|cell| cell.symbol() == "C")
        .expect("completed marker");
    assert!(completed.style().add_modifier.contains(Modifier::DIM));
}
