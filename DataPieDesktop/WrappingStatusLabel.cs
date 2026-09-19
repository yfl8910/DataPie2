using System;
using System.Drawing;
using System.Windows.Forms;

namespace DataPieDesktop
{
    internal sealed class WrappingStatusLabel : ToolStripStatusLabel
    {
        private const TextFormatFlags Format = TextFormatFlags.WordBreak | TextFormatFlags.TextBoxControl |
            TextFormatFlags.NoPrefix | TextFormatFlags.EndEllipsis;

        public int MeasureHeight(int width)
        {
            int lineHeight = TextRenderer.MeasureText("Ag", Font, Size.Empty, TextFormatFlags.NoPadding).Height;
            int height = TextRenderer.MeasureText(Text, Font, new Size(Math.Max(1, width), int.MaxValue), Format).Height;
            return Math.Clamp(height, lineHeight, lineHeight * 3);
        }

        protected override void OnPaint(PaintEventArgs e)
        {
            using var background = new SolidBrush(BackColor);
            e.Graphics.FillRectangle(background, new Rectangle(Point.Empty, Size));
            TextRenderer.DrawText(e.Graphics, Text, Font, new Rectangle(Point.Empty, Size), ForeColor, Format);
        }
    }
}
