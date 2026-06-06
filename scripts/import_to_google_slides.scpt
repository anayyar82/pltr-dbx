-- Import ATT_SDP_Architecture.pdf into an open Google Slides deck.
-- Requires: Chrome logged into Google, Accessibility permission for Script Editor/Terminal.

property presentationURL : "https://docs.google.com/presentation/d/1qj_BWxHYc7WIeIclSm7NhQ7driRx0MY0iYr_BEUrXXM/edit"
property pdfPath : "/Users/ankur.nayyar/Documents/pltr-dbx/docs/ATT_SDP_Architecture.pdf"

tell application "Google Chrome"
	activate
	if (count of windows) = 0 then make new window
	set URL of active tab of front window to presentationURL
end tell

delay 6

tell application "System Events"
	tell process "Google Chrome"
		set frontmost to true
		-- File → Import slides…
		try
			click menu item "Import slides…" of menu "File" of menu bar 1
		on error
			try
				click menu item "Import slides..." of menu "File" of menu bar 1
			on error errMsg
				return "Could not open Import slides menu: " & errMsg
			end try
		end try
	end tell
end tell

delay 3

-- Upload tab + file picker
tell application "System Events"
	tell process "Google Chrome"
		try
			click button "Upload" of group 1 of window 1
		end try
	end tell
	-- macOS open dialog: Cmd+Shift+G → path → Return → Open
	keystroke "g" using {command down, shift down}
	delay 0.8
	keystroke pdfPath
	delay 0.3
	key code 36 -- Return (Go)
	delay 0.8
	keystroke "a" using command down -- select file if needed
	delay 0.3
	keystroke return -- Open
	delay 4
	-- Select all imported slides + Replace all (if dialog appears)
	try
		keystroke "a" using command down
		delay 0.5
		click button "Import slides" of window 1 of process "Google Chrome"
	on error
		try
			click button "Replace presentation" of window 1 of process "Google Chrome"
		end try
	end try
end tell

return "Import triggered — confirm Replace all slides in Chrome if prompted."
