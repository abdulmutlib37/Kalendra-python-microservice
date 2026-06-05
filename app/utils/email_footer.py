"""Kalendra branded email builder — body + footer in one unified HTML document.

Keeping everything in a single <html>/<body> prevents email clients (Gmail,
Outlook, Apple Mail) from treating the footer as trailing quoted content and
hiding it behind "Show quoted text".
"""

from __future__ import annotations

import html as _html


def build_html_email(plain_text_body: str, sender_name: str) -> str:
    """Return a complete HTML email with the message body and Kalendra footer.

    Body paragraphs are split on blank lines, HTML-escaped, and rendered as
    <p> blocks.  The footer is inlined into the same <body> so email clients
    never treat it as quoted/trailing content.
    """
    paragraphs = plain_text_body.strip().split("\n\n")
    body_html_parts: list[str] = []
    for para in paragraphs:
        lines = para.strip().splitlines()
        escaped_lines = [_html.escape(line) for line in lines]
        body_html_parts.append("<br>".join(escaped_lines))

    body_html = "".join(
        f'<p style="margin:0 0 14px 0;font-family:Arial,\'Helvetica Neue\',Helvetica,sans-serif;'
        f'font-size:14px;line-height:1.6;color:#1a1a1a;">{p}</p>'
        for p in body_html_parts
        if p.strip()
    )

    safe_name = _html.escape(sender_name) if sender_name else "[Name]"

    return f"""\
<!DOCTYPE html>
<html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office" lang="en">
<head>
  <title></title>
  <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="color-scheme" content="light only">
  <meta name="supported-color-schemes" content="light only">
  <!--[if mso]>
  <xml><w:WordDocument xmlns:w="urn:schemas-microsoft-com:office:word"><w:DontUseAdvancedTypographyReadingMail/></w:WordDocument>
  <o:OfficeDocumentSettings><o:PixelsPerInch>96</o:PixelsPerInch><o:AllowPNG/></o:OfficeDocumentSettings></xml>
  <![endif]-->
  <!--[if !mso]><!-->
  <link href="https://fonts.googleapis.com/css2?family=Fira+Sans:wght@100;200;300;400;500;600;700;800;900" rel="stylesheet" type="text/css">
  <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@100;200;300;400;500;600;700;800;900" rel="stylesheet" type="text/css">
  <!--<![endif]-->
  <style>
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; padding: 0; color-scheme: light; }}
    a[x-apple-data-detectors] {{ color: inherit !important; text-decoration: inherit !important; }}
    #MessageViewBody a {{ color: inherit; text-decoration: none; }}
    p {{ line-height: inherit; }}
    .desktop_hide, .desktop_hide table {{ mso-hide: all; display: none; max-height: 0px; overflow: hidden; }}
    .image_block img+div {{ display: none; }}
    sup, sub {{ font-size: 75%; line-height: 0; }}
    @media (max-width: 700px) {{
      .row-content {{ width: 100% !important; }}
      .stack .column {{ width: 100%; display: block; }}
      .mobile_hide {{ min-height: 0; max-height: 0; max-width: 0; display: none; overflow: hidden; font-size: 0; }}
      .desktop_hide, .desktop_hide table {{ display: table !important; max-height: none !important; }}
      .row-2 .column-1 .block-1.heading_block h3 {{ text-align: center !important; font-size: 18px !important; }}
      .row-2 .column-1 .block-1.heading_block td.pad {{ padding: 4px !important; }}
      .row-2 .column-2 .block-1.image_block td.pad div {{ margin: 0 auto !important; }}
      .row-2 .column-1 .block-2.paragraph_block td.pad > div {{ text-align: center !important; font-size: 9px !important; }}
      .row-2 .column-1 .block-2.paragraph_block td.pad {{ padding: 0 15px 4px 15px !important; }}
      .row-1 .column-1 .col-pad {{ padding: 4px 10px 0 10px !important; }}
    }}
    /* Prevent dark-mode clients from inverting the logo image */
    @media (prefers-color-scheme: dark) {{
      .kalendra-logo img {{
        filter: none !important;
        -webkit-filter: none !important;
      }}
    }}
    <!--[if mso ]><style>sup, sub {{ font-size: 100% !important; }} sup {{ mso-text-raise:10%; }} sub {{ mso-text-raise:-10%; }}</style><![endif]-->
  </style>
</head>
<body class="body" style="margin:0;padding:0;-webkit-text-size-adjust:none;text-size-adjust:none;background-color:transparent;">

  <!-- ===== EMAIL BODY ===== -->
  <table width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation"
         style="mso-table-lspace:0pt;mso-table-rspace:0pt;background-color:transparent;">
    <tbody><tr><td>
      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation"
             style="mso-table-lspace:0pt;mso-table-rspace:0pt;width:680px;margin:0 auto;" width="680">
        <tbody><tr>
          <td style="padding:24px 24px 16px 24px;font-family:Arial,'Helvetica Neue',Helvetica,sans-serif;font-size:14px;line-height:1.6;color:#1a1a1a;">
            {body_html}
          </td>
        </tr></tbody>
      </table>
    </td></tr></tbody>
  </table>

  <!-- ===== KALENDRA FOOTER ===== -->
  <table class="nl-container" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation"
         style="mso-table-lspace:0pt;mso-table-rspace:0pt;background-color:transparent;">
    <tbody><tr><td>

      <!-- Divider Row -->
      <table class="row row-1" align="center" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation"
             style="mso-table-lspace:0pt;mso-table-rspace:0pt;background-color:transparent;background-size:auto;">
        <tbody><tr><td>
          <table class="row-content stack" align="center" border="0" cellpadding="0" cellspacing="0" role="presentation"
                 style="mso-table-lspace:0pt;mso-table-rspace:0pt;background-color:transparent;background-size:auto;color:#000000;width:680px;margin:0 auto;" width="680">
            <tbody><tr>
              <td class="column column-1" width="100%"
                  style="mso-table-lspace:0pt;mso-table-rspace:0pt;font-weight:400;text-align:left;vertical-align:top;">
                <table width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation"
                       style="mso-table-lspace:0pt;mso-table-rspace:0pt;">
                  <tr>
                    <td class="col-pad" style="padding-top:4px;">
                      <table class="divider_block block-1" width="100%" border="0" cellpadding="8" cellspacing="0" role="presentation"
                             style="mso-table-lspace:0pt;mso-table-rspace:0pt;">
                        <tr>
                          <td class="pad" align="center">
                            <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%"
                                   style="mso-table-lspace:0pt;mso-table-rspace:0pt;">
                              <tr>
                                <td class="divider_inner"
                                    style="font-size:1px;line-height:1px;border-top:1px solid #dddddd;">
                                  <span style="word-break:break-word;">&#8202;</span>
                                </td>
                              </tr>
                            </table>
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                </table>
              </td>
            </tr></tbody>
          </table>
        </td></tr></tbody>
      </table>

      <!-- Content Row: Heading + Logo -->
      <table class="row row-2" align="center" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation"
             style="mso-table-lspace:0pt;mso-table-rspace:0pt;">
        <tbody><tr><td>
          <table class="row-content stack" align="center" border="0" cellpadding="0" cellspacing="0" role="presentation"
                 style="mso-table-lspace:0pt;mso-table-rspace:0pt;border-radius:0;color:#000000;width:680px;margin:0 auto;" width="680">
            <tbody><tr>

              <!-- Text column (66%) -->
              <td class="column column-1" width="66.66666666666667%"
                  style="mso-table-lspace:0pt;mso-table-rspace:0pt;font-weight:400;text-align:left;vertical-align:top;">
                <table width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation"
                       style="mso-table-lspace:0pt;mso-table-rspace:0pt;">
                  <tr>
                    <td class="col-pad" style="padding-bottom:4px;padding-top:4px;">
                      <table class="heading_block block-1" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation"
                             style="mso-table-lspace:0pt;mso-table-rspace:0pt;">
                        <tr>
                          <td class="pad" style="width:100%;padding-top:8px;padding-bottom:8px;" align="center">
                            <h3 style="margin:0;color:#2e363c;direction:ltr;font-family:'Montserrat','Trebuchet MS','Lucida Grande','Lucida Sans Unicode','Lucida Sans',Tahoma,sans-serif;font-size:22px;font-weight:700;letter-spacing:normal;line-height:1.2;text-align:left;margin-top:0;margin-bottom:0;mso-line-height-alt:26px;">
                              <span style="word-break:break-word;">From the desk of Kalendra</span>
                            </h3>
                          </td>
                        </tr>
                      </table>
                      <table class="paragraph_block block-2" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation"
                             style="mso-table-lspace:0pt;mso-table-rspace:0pt;word-break:break-word;">
                        <tr>
                          <td class="pad">
                            <div style="color:#444a5b;direction:ltr;font-family:Arial,'Helvetica Neue',Helvetica,sans-serif;font-size:12px;font-weight:400;letter-spacing:0px;line-height:1.5;text-align:left;mso-line-height-alt:18px;">
                              <p style="margin:0;">This message was sent by an AI assistant. To reach {safe_name} directly, say&nbsp;<em>"connect with {safe_name}"</em></p>
                            </div>
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                </table>
              </td>

              <!-- Logo column (33%) -->
              <td class="column column-2" width="33.333333333333336%"
                  style="mso-table-lspace:0pt;mso-table-rspace:0pt;font-weight:400;text-align:left;vertical-align:top;">
                <table width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation"
                       style="mso-table-lspace:0pt;mso-table-rspace:0pt;">
                  <tr>
                    <td class="col-pad" style="padding-bottom:4px;padding-top:4px;">
                      <table class="image_block block-1" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation"
                             style="mso-table-lspace:0pt;mso-table-rspace:0pt;">
                        <tr>
                          <td class="pad" style="width:100%;padding-top:4px;padding-right:5px;" align="left">
                            <div class="kalendra-logo" style="max-width:60px;">
                              <a href="https://getkalendra.com/" target="_blank">
                                <!--[if mso]><v:image xmlns:v="urn:schemas-microsoft-com:vml"
                                  src="https://e29c67650f.imgdist.com/pub/bfra/a1cmm4cr/ijn/odr/yqs/Group%201000000862%20%282%29.png"
                                  style="width:60px;height:auto;border-radius:18px;" /><![endif]-->
                                <!--[if !mso]><!-->
                                <img src="https://e29c67650f.imgdist.com/pub/bfra/a1cmm4cr/ijn/odr/yqs/Group%201000000862%20%282%29.png"
                                     style="display:block;height:auto;border:0;width:100%;border-radius:18px;filter:none !important;-webkit-filter:none !important;"
                                     width="60" alt="Kalendra" title="Kalendra" height="auto">
                                <!--<![endif]-->
                              </a>
                            </div>
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                </table>
              </td>

            </tr></tbody>
          </table>
        </td></tr></tbody>
      </table>

    </td></tr></tbody>
  </table>
  <!-- End -->

</body>
</html>"""
